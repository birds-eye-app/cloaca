import os

from fastapi.responses import FileResponse


async def get_audio_file(location_code: str | None):
    try:
        audio_files_path = os.environ["AUDIO_FILES_PATH"]
    except KeyError:
        raise RuntimeError(
            "AUDIO_FILES_PATH environment variable is required. "
            "Please set it to the path of your audio files."
        )
    file_path = f"{audio_files_path}/{location_code}.wav"
    print(f"Looking for audio file at {file_path}")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="audio/wav")
    else:
        return {"message": "Audio file not found"}, 404
