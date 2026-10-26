"""
The logo images a testbed may upload.

The type is read from the file's own first bytes, never from its name or the Content-Type the browser sent: the
orchestrator serves these files to anyone, so what it stores has to be what it says it is.

A PDF is not stored. Browsers do not show one as an image, so its first page is rendered to a PNG on upload (cropped to
what is drawn on it, transparent background) and only that PNG is kept and served. SVG is left out on purpose: opened
directly in a browser it can run scripts.
"""

MAX_LOGO_BYTES = 512 * 1024          # a stored image; also the largest PNG, JPEG, WebP or GIF accepted
MAX_PDF_BYTES = 2 * 1024 * 1024      # a PDF logo may be larger than the bitmap it becomes
ALLOWED = "PNG, JPEG, WebP, GIF or PDF"
PDF = "application/pdf"

# The long side of the PNG a PDF becomes, tried in turn until the result fits MAX_LOGO_BYTES.
_RENDER_SIZES = (512, 384, 256, 128)
_MARGIN = 0.02   # of the drawn area, kept around it when a PDF page is cropped


def sniff(data: bytes) -> str | None:
    """The media type of a PNG, JPEG, GIF or WebP image or a PDF, judged by its magic bytes; None for anything else."""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"%PDF-"):
        return PDF
    return None


class PdfError(ValueError):
    """The PDF cannot be made into a logo; the message says why."""


class PdfTooLarge(PdfError):
    """Even the smallest rendering of the page is over MAX_LOGO_BYTES."""


def pdf_to_png(data: bytes) -> bytes:
    """The first page of a PDF as a PNG, cropped to what is drawn on it, with a transparent background.

    Raises PdfError for a PDF that is encrypted, has no pages, is damaged or draws nothing, and PdfTooLarge when the
    picture does not fit MAX_LOGO_BYTES at any of the sizes tried.
    """
    import pymupdf

    try:
        doc = pymupdf.open(stream=data, filetype="pdf")
    except Exception as e:
        raise PdfError(f"the file is not a readable PDF ({e})") from e
    try:
        if doc.needs_pass or doc.is_encrypted:
            raise PdfError("the PDF is password-protected")
        if doc.page_count < 1:
            raise PdfError("the PDF has no pages")
        page = doc.load_page(0)
        area = _drawn_area(page)
        if area is None or area.is_empty or area.width < 1 or area.height < 1:
            raise PdfError("the first page of the PDF has nothing on it")
        for size in _RENDER_SIZES:
            zoom = size / max(area.width, area.height)
            png = page.get_pixmap(matrix=pymupdf.Matrix(zoom, zoom), clip=area, alpha=True).tobytes("png")
            if len(png) <= MAX_LOGO_BYTES:
                return png
        raise PdfTooLarge(f"the picture on the first page is larger than {MAX_LOGO_BYTES // 1024} KB even when small")
    except PdfError:
        raise
    except Exception as e:
        raise PdfError(f"the first page could not be drawn ({e})") from e
    finally:
        doc.close()


def _drawn_area(page):
    """The page's drawn content (paths, text, images) with a small margin; the whole page if what is drawn cannot be
    told, None if nothing is drawn. A logo on a whole A4 sheet would otherwise become a thumbnail in the middle of a
    mostly empty picture."""
    import pymupdf

    whole = page.rect
    try:
        # Not the clip regions or invisible text (OCR layers): they say nothing about what is drawn.
        boxes = [pymupdf.Rect(b[1]) for b in page.get_bboxlog()
                 if not b[0].startswith(("clip", "ignore")) and not pymupdf.Rect(b[1]).is_empty]
    except Exception:
        return whole
    if not boxes:
        return None  # nothing is drawn on the page
    area = boxes[0]
    for b in boxes[1:]:
        area |= b
    margin = max(area.width, area.height) * _MARGIN
    return pymupdf.Rect(area.x0 - margin, area.y0 - margin, area.x1 + margin, area.y1 + margin) & whole
