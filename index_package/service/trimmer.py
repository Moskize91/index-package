from __future__ import annotations
from dataclasses import dataclass

from ..parser import PdfParser
from ..index import Index, IndexNode, IndexSegment, IndexNodeMatching

@dataclass
class PageQueryItem:
  pdf_files: list[PagePDFFile]
  distance: float
  content: str
  segments: list[PageHighlightSegment]
  annotations: list[PageAnnoQueryItem]

@dataclass
class PagePDFFile:
  pdf_path: str
  page_index: int

@dataclass
class PageAnnoQueryItem:
  index: int
  distance: float
  content: str
  segments: list[PageHighlightSegment]

@dataclass
class PageHighlightSegment:
  start: int
  end: int
  highlights: list[tuple[int, int]]

def trim_nodes(index: Index, pdf_parser: PdfParser, nodes: list[IndexNode]) -> list[PageQueryItem]:
  query_items: list[PageQueryItem] = []
  query_items_dict: dict[str, PageQueryItem] = {}

  for node in nodes:
    page = pdf_parser.page(node.id)
    if page is None:
      continue

    if node.type == "pdf.page.anno.content":
      id_cells = node.id.split("/")
      page_hash = id_cells[0]
      page_index = int(id_cells[2])
      anno_content = page.annotations[page_index].content
      query_item = query_items_dict.get(page_hash, None)
      if anno_content is not None and query_item is not None:
        anno_item = PageAnnoQueryItem(
          index=page_index,
          distance=node.vector_distance,
          content=anno_content,
          segments=_mark_highlights(
            content=anno_content,
            segments=node.segments,
            ignore_empty_segments=node.matching != IndexNodeMatching.Similarity,
          ),
        )
        query_item.annotations.append(anno_item)

    elif node.type == "pdf.page":
      content = page.snapshot
      query_item = PageQueryItem(
        pdf_files=[],
        distance=node.vector_distance,
        content=content,
        annotations=[],
        segments=_mark_highlights(
          content=content,
          segments=node.segments,
          ignore_empty_segments=node.matching != IndexNodeMatching.Similarity,
        ),
      )
      for relative_to in index.get_page_relative_to_pdf(page.hash):
        query_item.pdf_files.append(PagePDFFile(
          pdf_path=relative_to.pdf_path,
          page_index=relative_to.page_index,
        ))
      query_items_dict[page.hash] = query_item
      query_items.append(query_item)

  for query_item in query_items:
    query_item.annotations.sort(key=lambda item: item.index)

  return query_items

def _mark_highlights(content: str, segments: list[IndexSegment], ignore_empty_segments: bool) -> list[PageHighlightSegment]:
  content = content.lower()
  highlight_segments: list[PageHighlightSegment] = []
  for segment in segments:
    start = segment.start
    end = segment.end
    highlights: list[tuple[int, int]] = []
    for token in segment.matched_tokens:
      for highlight in _search_highlights(token, start, end, content):
        highlights.append(highlight)

    if not ignore_empty_segments or len(highlights) > 0:
      highlights.sort(key=lambda h: h[0])
      highlight_segments.append(PageHighlightSegment(
        start=start,
        end=end,
        highlights=highlights,
      ))
  return highlight_segments

def _search_highlights(token: str, start: int, end: int, content: str):
  finding_start = start
  while finding_start < end:
    index = content.find(token, finding_start, end)
    if index == -1:
      break
    offset = index - start
    finding_start = index + len(token)
    yield (offset, offset + len(token))