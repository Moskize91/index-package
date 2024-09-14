import os

from termcolor import colored
from index_package import Service, PageQueryItem

def show_items(text: str, pages: list[PageQueryItem]):
  # command will see the bottom item first
  pages.reverse()
  items_count = 0

  for page in pages:
    print(colored(_split_str("-"), "dark_grey"))
    print(f"PDF File Page at page {page.index + 1}:")
    print(f"Rank: {page.rank}")

    if len(page.contents) > 0:
      print(f"Found Contents: {len(page.contents)}")

    if len(page.annotations) > 0:
      print(f"Found Annotations: {len(page.annotations)}")

    if len(page.pdf_files) > 0:
      print("Files:")
      for file in page.pdf_files:
        file_path = colored(file, "dark_grey")
        print(f"  {file_path}")

    if (len(page.contents) > 0):
      print(colored(_split_str("-"), "dark_grey"))
      items_count += len(page.contents)
      print(_highlight_text(
        text=page.content,
        segments=[(c.start, c.end) for c in page.contents]
      ))

    if len(page.annotations) > 0:
      print(colored(_split_str("-"), "dark_grey"))
      for i, anno in enumerate(page.annotations):
        items_count += 1
        print(f"Annotation Index: {anno.index + 1}")
        print(f"Rank: {anno.rank}")
        print(_highlight_text(
          text=anno.content,
          segments=[(anno.start, anno.end)]
        ))
        if i < len(page.annotations) - 1:
          print("")

    print("\n")

  print(colored(_split_str("-"), "dark_grey"))
  print(f"Query: {text}")
  print(f"Found {len(pages)} Pages and {items_count} Records")

def _highlight_text(text: str, segments: list[tuple[int, int]]) -> str:
  segments.sort(key=lambda x: x[0])

  buffer: list[str] = []
  latest_end: int = 0

  for start, end in segments:
    if start > latest_end:
      background_text = text[latest_end:start]
      buffer.append(colored(background_text, "dark_grey"))
      latest_end = start
    if end > start:
      buffer.append(text[start:end])
      latest_end = end

  if latest_end < len(text):
    background_text = text[latest_end:]
    buffer.append(colored(background_text, "dark_grey"))

  return "".join(buffer)

def _split_str(char: str) -> str:
  return os.get_terminal_size().columns * char