from __future__ import annotations

import math

from typing import Optional
from dataclasses import dataclass
from ..parser import PdfParser
from ..index import Index, IndexNode

@dataclass
class _Pdf:
  hash: str
  file_paths: list[str]

@dataclass
class _PageCollection:
  pdf_hash: str
  page_index: int
  segments: list[_PageSegment]
  anno_content_list: list[_AnnoCollection]
  anno_extracted_list: list[_AnnoCollection]
  pdf: Optional[_Pdf] = None
  rank: float = 0.0

@dataclass
class _PageSegment:
  rank: float
  segment: tuple[int, int]

@dataclass
class _AnnoCollection:
  index: int
  rank: float
  segment: tuple[int, int]

@dataclass
class PageQueryItem:
  index: int
  pdf_files: list[str]
  rank: float
  content: str
  contents: list[PageContentQueryItem]
  annotations: list[PageAnnoQueryItem]

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

class Trimmer:
  def __init__(self, pdf_parser: PdfParser, index: Index, nodes: dict[str, list[IndexNode]]):
    self._pdf_parser: PdfParser = pdf_parser
    self._index: Index = index
    self._items: dict[str, list[PdfQueryItem]] = items
    self._pdfs: dict[str, _Pdf] = {}
    self._page_collections: dict[tuple[str, int], _PageCollection] = {}

  def do(self) -> list[PageQueryItem]:
    self._save_items_into_page_collections()
    self._collect_infos_of_pdf()

    return self._response_final_items(
      page_collections=self._count_rank_and_sort(),
    )

  def _save_items_into_page_collections(self):
    for item in self._items["fts5"]:
      kind = item.kind
      if kind == PdfQueryKind.page:
        self._page_collection(item).segments.append(
          _PageSegment(
            rank=item.rank,
            segment=(item.segment_start, item.segment_end),
          )
        )
      if kind == PdfQueryKind.anno_content:
        self._page_collection(item).anno_content_list.append(
          _AnnoCollection(
            index=item.anno_index,
            rank=item.rank,
            segment=(item.segment_start, item.segment_end),
          )
        )
      if kind == PdfQueryKind.anno_extracted:
        self._page_collection(item).anno_extracted_list.append(
          _AnnoCollection(
            index=item.anno_index,
            rank=item.rank,
            segment=(item.segment_start, item.segment_end),
          )
        )

  def _page_collection(self, item: PdfQueryItem):
    pdf_hash = item.pdf_hash
    page_index = item.page_index
    collection = self._page_collections.get((pdf_hash, page_index), None)
    if collection is None:
      collection = _PageCollection(
        pdf_hash=pdf_hash,
        page_index=page_index,
        segments=[],
        anno_content_list=[],
        anno_extracted_list=[],
      )
      self._page_collections[(pdf_hash, page_index)] = collection
    return collection

  def _collect_infos_of_pdf(self):
    for item in self._page_collections.values():
      pdf_hash = item.pdf_hash
      pdf = self._pdfs.get(pdf_hash, None)
      if pdf is None:
        pdf = _Pdf(
          hash=pdf_hash,
          file_paths=self._index.get_paths(pdf_hash),
        )
        self._pdfs[pdf_hash] = pdf
      item.pdf = pdf

  def _count_rank_and_sort(self) -> list[_PageCollection]:
    page_collections: list[_PageCollection] = []

    for page_collection in self._page_collections.values():
      page_collection.segments.sort(key=lambda x: x.rank)
      page_collection.anno_content_list.sort(key=lambda x: x.rank)
      page_collection.anno_extracted_list.sort(key=lambda x: x.rank)
      page_rank = float("inf")

      if len(page_collection.segments) > 0:
        page_rank = min(page_rank, page_collection.segments[0].rank)
      if len(page_collection.anno_content_list) > 0:
        page_rank = min(page_rank, page_collection.anno_content_list[0].rank)
      if math.isinf(page_rank):
        continue

      page_collection.rank = page_rank
      page_collections.append(page_collection)

    page_collections.sort(key=lambda x: x.rank)
    return page_collections


  def _response_final_items(self, page_collections: list[_PageCollection]) -> list[PageQueryItem]:
    items: list[PageQueryItem] = []

    for page_collection in page_collections:
      assert page_collection.pdf is not None
      page = self._pdf_parser.page(page_collection.pdf.hash, page_collection.page_index)
      if page is None:
        continue
      item = PageQueryItem(
        pdf_files = page_collection.pdf.file_paths,
        index=page_collection.page_index,
        rank = page_collection.rank,
        content = page.snapshot,
        contents = [],
        annotations = [],
      )
      for segment in page_collection.segments:
        item.contents.append(
          PageContentQueryItem(
            start=segment.segment[0],
            end=segment.segment[1],
            rank=segment.rank,
          )
        )
      for anno_content in page_collection.anno_content_list:
        anno = page.annotations[anno_content.index]
        if anno.content is None:
          continue
        item.annotations.append(
          PageAnnoQueryItem(
            index=anno_content.index,
            start=anno_content.segment[0],
            end=anno_content.segment[1],
            rank=anno_content.rank,
            content=anno.content,
          )
        )
      items.append(item)

    return items