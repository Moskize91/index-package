from __future__ import annotations
from dataclasses import dataclass
from ..parser import PdfParser
from ..index import Index, IndexNode

@dataclass
class PageQueryItem:
  pdf_files: list[PagePDFFile]
  rank: float
  content: str
  segments: list[ContentSegment]
  annotations: list[PageAnnoQueryItem]

@dataclass
class PagePDFFile:
  pdf_path: str
  page_index: int

@dataclass
class PageAnnoQueryItem:
  index: int
  rank: float
  content: str
  segments: list[ContentSegment]

@dataclass
class ContentSegment:
  start: int
  end: int

def trim_nodes(index: Index, pdf_parser: PdfParser, nodes: list[IndexNode]) -> list[PageQueryItem]:
  query_items: list[PageQueryItem] = []
  query_items_dict: dict[str, PageQueryItem] = {}

  for node in nodes:
    type = node.metadata.get("type", None)
    page = pdf_parser.page(node.id)

    if page is None:
      continue

    if type == "pdf.page.anno.content":
      id_cells = node.id.split("/")
      page_hash = id_cells[0]
      page_index = int(id_cells[2])
      anno_content = page.annotations[page_index].content
      query_item = query_items_dict.get(page_hash, None)
      if anno_content is not None and query_item is not None:
        anno_item = PageAnnoQueryItem(
          index=page_index,
          rank=node.rank,
          content=anno_content,
          segments=[],
        )
        query_item.annotations.append(anno_item)
        for start, end in node.segments:
          anno_item.segments.append(ContentSegment(
            start=start,
            end=end,
          ))
    elif type == "pdf.page":
      query_item = PageQueryItem(
        pdf_files=[],
        rank=node.rank,
        content=page.snapshot,
        segments=[],
        annotations=[],
      )
      for relative_to in index.get_page_relative_to_pdf(page.hash):
        query_item.pdf_files.append(PagePDFFile(
          pdf_path=relative_to.pdf_path,
          page_index=relative_to.page_index,
        ))
      for start, end in node.segments:
        query_item.segments.append(ContentSegment(
          start=start,
          end=end,
        ))
      query_items_dict[page.hash] = query_item
      query_items.append(query_item)

  for query_item in query_items:
    query_item.annotations.sort(key=lambda item: item.index)

  return query_items