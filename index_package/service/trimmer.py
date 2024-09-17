from __future__ import annotations
from dataclasses import dataclass
from ..parser import PdfParser
from ..index import Index, IndexNode

@dataclass
class PageQueryItem:
  pdf_files: list[PagePDFFile]
  rank: float
  content: str
  contents: list[PageContentQueryItem]
  annotations: list[PageAnnoQueryItem]

@dataclass
class PagePDFFile:
  pdf_path: str
  page_index: int

@dataclass
class PageContentQueryItem:
  start: int
  end: int
  rank: float

@dataclass
class PageAnnoQueryItem:
  index: int
  start: int
  end: int
  rank: float
  content: str

def trim_nodes(index: Index, pdf_parser: PdfParser, nodes: list[IndexNode]) -> list[PageQueryItem]:
  query_items: list[PageQueryItem] = []

  for node in nodes:
    page = pdf_parser.page(node.id)
    if page is None:
      continue

    query_item = PageQueryItem(
      pdf_files=[],
      rank=node.rank,
      content=page.snapshot,
      contents=[],
      annotations=[],
    )
    for relative_to in index.get_page_relative_to_pdf(page.hash):
      query_item.pdf_files.append(PagePDFFile(
        pdf_path=relative_to.pdf_path,
        page_index=relative_to.page_index,
      ))
    for start, end in node.segments:
      query_item.contents.append(PageContentQueryItem(
        start=start,
        end=end,
        rank=node.rank,
      ))
    # TODO: to fill annotation
    query_items.append(query_item)

  return query_items