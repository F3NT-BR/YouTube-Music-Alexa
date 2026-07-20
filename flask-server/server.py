import asyncio
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from urllib.parse import parse_qs, urlparse

from flask import Flask, jsonify, render_template, request
from ytmusicapi import YTMusic

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("youtube-music-alexa")

COOKIES_SOURCE_PATH = "/etc/secrets/cookies.txt"
COOKIES_WORK_PATH = "/tmp/youtube-cookies.txt"
COOKIES_LOCK = threading.Lock()


class Supporting:
    @staticmethod
    def _get_thumbnail(track: dict):
        thumbnails = track.get("thumbnails") or track.get("thumbnail") or []
        if not thumbnails:
            return None

        thumbnail = thumbnails[-1]
        return {
            "url": thumbnail.get("url", ""),
            "width": int(thumbnail.get("width") or 0),
            "height": int(thumbnail.get("height") or 0),
        }

    @staticmethod
    def _get_metadata(track: dict):
        video_id = track.get("videoId")
        title = track.get("title")

        if not video_id or not title:
            return None

        artists = track.get("artists") or []
        artist_names = [
            artist.get("name")
            for artist in artists
            if isinstance(artist, dict) and artist.get("name")
        ]

        return {
            "title": title,
            "artist": " e ".join(artist_names) or "Artista desconhecido",
            "video_id": video_id,
            "thumbnail": Supporting._get_thumbnail(track),
        }

    @staticmethod
    async def get_radiolist(song_name: str):
        ytmusic = YTMusic()

        search_results = await asyncio.to_thread(
            ytmusic.search,
            query=song_name,
            filter="songs",
            ignore_spelling=True,
        )

        first_result = next(
            (result for result in search_results if result.get("videoId")),
            None,
        )
        if not first_result:
            return None

        radio_results = await asyncio.to_thread(
            ytmusic.get_watch_playlist,
            videoId=first_result["videoId"],
            radio=True,
            limit=25,
        )

        playlist = [
            metadata
            for track in radio_results.get("tracks", [])
            if (metadata := Supporting._get_metadata(track)) is not None
        ]

        return playlist or None

    @staticmethod
    async def get_artist(artist_name: str):
        ytmusic = YTMusic()

        search_results = await asyncio.to_thread(
            ytmusic.search,
            query=artist_name,
            filter="songs",
            ignore_spelling=True,
            limit=25,
        )

        playlist = [
            metadata
            for track in search_results
            if (metadata := Supporting._get_metadata(track)) is not None
        ]

        return playlist or None

    @staticmethod
    async def get_album(album_name: str):
        ytmusic = YTMusic()

        search_results = await asyncio.to_thread(
            ytmusic.search,
            query=album_name,
            filter="albums",
            ignore_spelling=True,
            limit=10,
        )

        first_album = next(
            (album for album in search_results if album.get("browseId")),
            None,
        )
        if not first_album:
            return None

        album_results = await asyncio.to_thread(
            ytmusic.get_album,
            browseId=first_album["browseId"],
        )

        playlist = [
            metadata
            for track in album_results.get("tracks", [])
            if (metadata := Supporting._get_metadata(track)) is not None
        ]

        return playlist or None

    @staticmethod
    async def stream_playlist(playlist_id: str):
        ytmusic = YTMusic()

        playlist_data = await asyncio.to_thread(
            ytmusic.get_playlist,
            playlistId=playlist_id,
            limit=100,
        )

        playlist = [
            metadata
            for track in playlist_data.get("tracks", [])
            if (metadata := Supporting._get_metadata(track)) is not None
        ]

        if not playlist:
            return None

        stream = await Supporting.get_stream(playlist[0]["video_id"])
        if not stream:
            return None

        return {
            "song_info": {
                "metadata": playlist[0],
                "stream": stream,
            },
            "playlist": playlist,
        }

    @staticmethod
    def _prepare_writable_cookies():
        if not os.path.isfile(COOKIES_SOURCE_PATH):
            logger.warning(
                "Arquivo secreto de cookies não encontrado em %s.",
                COOKIES_SOURCE_PATH,
            )
            return None

        try:
            if not os.path.isfile(COOKIES_WORK_PATH):
                shutil.copyfile(COOKIES_SOURCE_PATH, COOKIES_WORK_PATH)
                os.chmod(COOKIES_WORK_PATH, 0o600)
                logger.info(
                    "Cookies copiados do segredo do Render para o diretório gravável."
                )

            return COOKIES_WORK_PATH
        except OSError:
            logger.exception("Não foi possível preparar a cópia gravável dos cookies.")
            return None

    @staticmethod
    async def get_stream(video_id: str):
        if not video_id or not re.fullmatch(r"[\w-]{6,20}", video_id):
            return None

        cookies_path = Supporting._prepare_writable_cookies()
        command = ["yt-dlp"]

        if cookies_path:
            command.extend(["--cookies", cookies_path])
            logger.info("Usando a cópia gravável dos cookies.")
        else:
            logger.warning("Executando o yt-dlp sem cookies.")

        command.extend(
            [
                "--js-runtimes",
                "deno",
                "--no-playlist",
                "--quiet",
                "--no-warnings",
                "--socket-timeout",
                "20",
                "--retries",
                "2",
                "-f",
                "bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]",
                "-g",
                f"https://www.youtube.com/watch?v={video_id}",
            ]
        )

        try:
            def run_yt_dlp():
                # Evita que duas requisições tentem atualizar o mesmo cookie jar ao mesmo tempo.
                with COOKIES_LOCK:
                    return subprocess.run(
                        command,
                        capture_output=True,
                        text=True,
                        timeout=45,
                        check=False,
                    )

            result = await asyncio.to_thread(run_yt_dlp)
        except subprocess.TimeoutExpired:
            logger.error("O yt-dlp excedeu o tempo limite para o vídeo %s.", video_id)
            return None
        except Exception:
            logger.exception("Erro ao executar o yt-dlp.")
            return None

        if result.returncode != 0:
            logger.error("Erro do yt-dlp: %s", result.stderr.strip())
            return None

        urls = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        if not urls:
            logger.error("O yt-dlp não retornou nenhuma URL de áudio.")
            return None

        return {"audio_url": urls[0]}

    @staticmethod
    async def find_stream_list(query: str, filter_name: str = "songs"):
        if filter_name == "songs":
            playlist = await Supporting.get_radiolist(query)
        elif filter_name == "artists":
            playlist = await Supporting.get_artist(query)
        elif filter_name == "albums":
            playlist = await Supporting.get_album(query)
        else:
            return None

        if not playlist:
            return None

        stream = await Supporting.get_stream(playlist[0]["video_id"])
        if not stream:
            return None

        return {
            "song_info": {
                "metadata": playlist[0],
                "stream": stream,
            },
            "playlist": playlist,
        }

    @staticmethod
    def playlist_url_to_id(value: str):
        value = (value or "").strip()
        if not value:
            return None

        parsed = urlparse(value)
        playlist_id = parse_qs(parsed.query).get("list", [None])[0]

        if not playlist_id and re.fullmatch(r"[\w-]+", value):
            playlist_id = value

        if not playlist_id or not re.fullmatch(r"[\w-]+", playlist_id):
            return None

        return playlist_id

    @staticmethod
    def playlist_url_to_encoded_id(url: str):
        playlist_id = Supporting.playlist_url_to_id(url)
        if not playlist_id:
            return None
        return Supporting.encode_to_hex(playlist_id)

    @staticmethod
    def encode_to_hex(value: str):
        return "".join(hex(ord(character))[2:].zfill(2) for character in value)

    @staticmethod
    async def get_playlist_info(playlist_id: str):
        ytmusic = YTMusic()

        playlist_data = await asyncio.to_thread(
            ytmusic.get_playlist,
            playlistId=playlist_id,
            limit=1,
        )

        if not playlist_data:
            return None

        return {
            "id": playlist_data.get("id") or playlist_id,
            "title": playlist_data.get("title") or "Playlist sem nome",
        }


@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "youtube-music-alexa"})


@app.route("/get_playlist_info/", methods=["GET"])
async def get_playlist_info():
    start_time = time.time()
    playlist_id = request.args.get("id", "").strip()

    if not playlist_id:
        return jsonify({"error": "Parâmetro id ausente."}), 400

    response = await Supporting.get_playlist_info(playlist_id)
    logger.info("get_playlist_info concluído em %.2fs", time.time() - start_time)

    if not response:
        return jsonify({"error": "Playlist não encontrada."}), 404

    return jsonify(response)


@app.route("/stream_playlist/", methods=["GET"])
async def stream_playlist():
    start_time = time.time()
    playlist_id = request.args.get("id", "").strip()

    if not playlist_id:
        return jsonify({"error": "Parâmetro id ausente."}), 400

    response = await Supporting.stream_playlist(playlist_id)
    logger.info("stream_playlist concluído em %.2fs", time.time() - start_time)

    if not response:
        return jsonify({"error": "Não foi possível carregar a playlist."}), 502

    return jsonify(response)


@app.route("/get_stream/", methods=["GET"])
async def get_stream():
    start_time = time.time()
    video_id = request.args.get("video_id", "").strip()

    if not video_id:
        return jsonify({"error": "Parâmetro video_id ausente."}), 400

    response = await Supporting.get_stream(video_id)
    logger.info("get_stream concluído em %.2fs", time.time() - start_time)

    if not response:
        return jsonify({"error": "Não foi possível obter o áudio."}), 502

    return jsonify(response)


@app.route("/find_stream_list/", methods=["GET"])
async def find_stream_list():
    start_time = time.time()
    query = request.args.get("query", "").strip()
    filter_name = request.args.get("filter", "songs").strip().lower()

    if not query:
        return jsonify({"error": "Parâmetro query ausente."}), 400

    response = await Supporting.find_stream_list(query, filter_name)
    logger.info("find_stream_list concluído em %.2fs", time.time() - start_time)

    if not response:
        return jsonify({"error": "Nenhuma música reproduzível foi encontrada."}), 502

    return jsonify(response)


@app.route("/setup/", methods=["GET", "POST"])
def index():
    hex_value = ""

    if request.method == "POST":
        apiurl_input = request.form.get("apiurl_input", "").strip()
        playlist_input = request.form.get("playlist_input", "").strip()

        if apiurl_input:
            hex_value = Supporting.encode_to_hex(apiurl_input)
        elif playlist_input:
            hex_value = Supporting.playlist_url_to_encoded_id(playlist_input)
            if not hex_value:
                hex_value = "Link ou ID da playlist inválido."
        else:
            hex_value = "Preencha um dos campos."

    return render_template("index.html", hex_value=hex_value)


@app.route("/privacy_policy/", methods=["GET"])
def privacy_policy():
    return render_template("privacy_policy.html")


@app.route("/terms_of_use/", methods=["GET"])
def terms_of_use():
    return render_template("terms_of_use.html")


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=False,
    )
