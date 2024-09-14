import os

from termcolor import colored
from index_package import Service, PdfQueryKind, PdfQueryItem


def show_items(text: list[str], service: Service, items: list[PdfQueryItem]):
  # command will see the bottom item first
  items.reverse()

  for item in items:
    print(colored(_split_str("="), "dark_grey"))
    if item.kind == PdfQueryKind.pdf:
      print(f"PDF Metadata")
    elif item.kind == PdfQueryKind.page:
      print(f"PDF page at page {item.page_index + 1}")
    elif item.kind == PdfQueryKind.anno_content:
      print(f"Annotation Content at Page {item.page_index + 1}")
    elif item.kind == PdfQueryKind.anno_extracted:
      print(f"Annotation Extracted Text at Page {item.page_index + 1}")
    print(f"Rank: {item.rank}")

    files = service.get_paths(item.pdf_hash)

    if len(files) > 0:
      print("Files:")
      for file in files:
        print(f"  {file}")

    print(colored(_split_str("-"), "dark_grey"))

    if item.kind == PdfQueryKind.page:
      content = service.page_content(item.pdf_hash, item.page_index)
      print(_colored_text(content, item.segment_start, item.segment_end))

    print("")

  query_text = " ".join(text)

  print(f"Query: {query_text}")
  print(f"Found {len(items)} results")

def _split_str(char: str) -> str:
  return os.get_terminal_size().columns * char

def _colored_text(text: str, start: int, end: int) -> str:
  prefix = colored(text[:start], "dark_grey")
  highlight = text[start:end]
  suffix = colored(text[end:], "dark_grey")
  return prefix + highlight + suffix