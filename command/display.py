import os

from typing import Optional
from termcolor import colored
from termcolor._types import Color
from index_package import PageHighlightSegment, QueryResult

def show_items(text: str, query_result: QueryResult):
  # command will see the bottom item first
  pages = query_result.page_items
  pages.reverse()
  keywords = ", ".join(query_result.keywords)
  items_count = 0

  for page in pages:
    print(colored(_split_str("-"), "dark_grey"))
    if len(page.pdf_files) == 0:
      pdf_file = page.pdf_files[0]
      pdf_file_path = colored(pdf_file.pdf_path, "dark_grey")
      print(f"PDF File page pat page {pdf_file.page_index + 1}: {pdf_file_path}")
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

    if len(page.segments) > 0:
      print(colored(_split_str("-"), "dark_grey"))
      items_count += len(page.segments)
      print(_highlight_text(
        text=page.content,
        segments=page.segments
      ))

    if len(page.annotations) > 0:
      print(colored(_split_str("-"), "dark_grey"))
      for i, anno in enumerate(page.annotations):
        items_count += 1
        print(f"Annotation Index: {anno.index + 1}")
        print(f"Distance: {anno.distance}")
        print(_highlight_text(
          text=anno.content,
          segments=anno.segments
        ))
        if i < len(page.annotations) - 1:
          print("")

    print("\n")

  print(colored(_split_str("-"), "dark_grey"))
  print(f"Query Keywords: {keywords}")
  print(f"Found {len(pages)} Pages and {items_count} Records")

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