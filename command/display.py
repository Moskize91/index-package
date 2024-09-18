import os

from typing import Optional
from termcolor import colored
from termcolor._types import Color
from index_package import PdfQueryItem, PageQueryItem, PageHighlightSegment, QueryResult

def show_items(query_result: QueryResult):
  # command will see the bottom item first
  items = query_result.items
  items.reverse()
  keywords = ", ".join(query_result.keywords)
  pages_count = 0
  records_count = 0

  for item in items:
    if isinstance(item, PdfQueryItem):
      _show_pdf_item(item)
      records_count += 1
    elif isinstance(item, PageQueryItem):
      records_count += _show_page_item(item)
      pages_count += 1
    print("\n")

  print(colored(_split_str("-"), "dark_grey"))
  print(f"Query Keywords: {keywords}")
  print(f"Found {len(items)} Pages and {records_count} Records")

def _show_pdf_item(pdf: PdfQueryItem):
  print(colored(_split_str("-"), "dark_grey"))
  if len(pdf.pdf_files) == 0:
    file_path = colored(pdf.pdf_files[0], "dark_grey")
    print(f"PDF File Metadata: {file_path}")
  else:
    print("PDF File Metadata from:")
    for pdf_file in pdf.pdf_files:
      file_path = colored(pdf_file, "dark_grey")
      print(f"  {file_path}")

  print(f"Distance: {pdf.distance}")

  metadata = pdf.metadata
  if metadata.author is not None:
    print(f"Author: {metadata.author}")
  if metadata.modified_at is not None:
    print(f"Title: {metadata.modified_at}")
  if metadata.producer is not None:
    print(f"Producer: {metadata.producer}")

def _show_page_item(page: PageQueryItem) -> int:
  count = 0
  print(colored(_split_str("-"), "dark_grey"))
  if len(page.pdf_files) == 0:
    pdf_file = page.pdf_files[0]
    pdf_file_path = colored(pdf_file.pdf_path, "dark_grey")
    print(f"PDF File page at page {pdf_file.page_index + 1}: {pdf_file_path}")
  else:
    print("PDF File page from:")
    for pdf_file in page.pdf_files:
      file_path = colored(pdf_file.pdf_path, "dark_grey")
      print(f"  page {pdf_file.page_index + 1} from {file_path}")

  if len(page.segments) > 0:
    print(f"Found Contents: {len(page.segments)}")

  if len(page.annotations) > 0:
    print(f"Found Annotations: {len(page.annotations)}")

  print(f"Distance: {page.distance}")
  print(colored(_split_str("-"), "dark_grey"))

  count += len(page.segments)
  print(_highlight_text(
    text=page.content,
    segments=page.segments
  ))

  if len(page.annotations) > 0:
    print(colored(_split_str("-"), "dark_grey"))
    for i, anno in enumerate(page.annotations):
      count += 1
      print(f"Annotation Index: {anno.index + 1}")
      print(f"Distance: {anno.distance}")
      print(_highlight_text(
        text=anno.content,
        segments=anno.segments
      ))
      if i < len(page.annotations) - 1:
        print("")

  return count

def _highlight_text(text: str, segments: list[PageHighlightSegment]) -> str:
  segments.sort(key=lambda x: x.start)

  buffer: list[str] = []
  latest_end: int = 0

  for segment in segments:
    start = segment.start
    end = segment.end
    if start > latest_end:
      background_text = text[latest_end:start]
      buffer.append(colored(background_text, "dark_grey"))
      latest_end = start
    if end > start:
      _mark_text(buffer, text[start:end], segment.highlights, mark_color="light_red")
      latest_end = end

  if latest_end < len(text):
    background_text = text[latest_end:]
    buffer.append(colored(background_text, "dark_grey"))

  return "".join(buffer)

def _mark_text(
  buffer: list[str],
  text: str,
  segments: list[tuple[int, int]],
  mark_color: Optional[Color] = None,
  background_color: Optional[Color] = None,
):
  latest_end: int = 0

  for start, end in segments:
    if start > latest_end:
      background_text = text[latest_end:start]
      if background_color is not None:
        background_text = colored(background_text, background_color)
      buffer.append(background_text)
      latest_end = start
    if end > start:
      marked_text = text[start:end]
      if mark_color is not None:
        marked_text = colored(marked_text, mark_color)
      buffer.append(marked_text)
      latest_end = end

  if latest_end < len(text):
    background_text = text[latest_end:]
    if background_color is not None:
      background_text = colored(background_text, background_color)
    buffer.append(background_text)


def _split_str(char: str) -> str:
  return os.get_terminal_size().columns * char