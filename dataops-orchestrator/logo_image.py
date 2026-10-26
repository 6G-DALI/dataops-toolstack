"""
The logo images a testbed may upload.

The type is read from the file's own first bytes, never from its name or the Content-Type the browser sent: the
orchestrator serves these files to anyone, so what it stores has to be what it says it is. SVG is left out on purpose:
opened directly in a browser it can run scripts.
"""

MAX_LOGO_BYTES = 512 * 1024
ALLOWED = "PNG, JPEG, WebP or GIF"


def sniff(data: bytes) -> str | None:
    """The media type of a PNG, JPEG, GIF or WebP image, judged by its magic bytes; None for anything else."""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None
