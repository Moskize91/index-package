import os
import unittest

from index_package.parser import PdfParser
from index_package.scanner import Event, EventKind, EventTarget
from index_package.segmentation import Segmentation
from index_package.index import Index, VectorDB, FTS5DB, PdfQueryKind
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_fts5_index_for_pdf(self):
    segmentation = Segmentation()
    parser = PdfParser(
      cache_dir_path=get_temp_path("index_fts5/parser_cache"),
      temp_dir_path=get_temp_path("index_fts5/temp"),
    )
    fts5_db = FTS5DB(
      index_dir_path=get_temp_path("index_fts5/fts5_db"),
    )
    index = Index(
      pdf_parser=parser,
      segmentation=segmentation,
      index_dir_path=get_temp_path("index_fts5/index"),
      databases=[fts5_db],
      sources={
        "assets": os.path.abspath(os.path.join(__file__, "../assets")),
      },
    )
    added_event = Event(
      id=0,
      kind=EventKind.Added,
      target=EventTarget.File,
      scope="assets",
      path="/The Analysis of the Transference.pdf",
      mtime=0,
    )
    index.handle_event(added_event)
    items = index.query("Extratransference", results_limit=100)["fts5"]
    items.sort(key=lambda x: x.page_index)

    self.assertEqual(len(items), 2)
    self.assertEqual([i.kind for i in items], [
      PdfQueryKind.page,
      PdfQueryKind.page,
    ])
    self.assertEqual([i.pdf_hash for i in items], [
      "8lMbEwyJykueeqwrQzrmFHt5usqlag47UpdUuFpXGfFptM13R3RUQ0AH0bCT93REjMbu25G43SFOduMehD_v8g==",
      "8lMbEwyJykueeqwrQzrmFHt5usqlag47UpdUuFpXGfFptM13R3RUQ0AH0bCT93REjMbu25G43SFOduMehD_v8g==",
    ])
    self.assertEqual([(i.page_index, i.anno_index) for i in items], [
      (0, 0), (1, 0)
    ])

  def test_vector_index_for_pdf(self):
    segmentation = Segmentation()
    parser = PdfParser(
      cache_dir_path=get_temp_path("index_vector/parser_cache"),
      temp_dir_path=get_temp_path("index_vector/temp"),
    )
    vector_db = VectorDB(
      index_dir_path=get_temp_path("index_vector/vector_db"),
      embedding_model_id="shibing624/text2vec-base-chinese",
    )
    index = Index(
      pdf_parser=parser,
      segmentation=segmentation,
      index_dir_path=get_temp_path("index_vector/index"),
      databases=[vector_db],
      sources={
        "assets": os.path.abspath(os.path.join(__file__, "../assets")),
      },
    )
    added_event = Event(
      id=0,
      kind=EventKind.Added,
      target=EventTarget.File,
      scope="assets",
      path="/The Analysis of the Transference.pdf",
      mtime=0,
    )
    index.handle_event(added_event)
    items = index.query("identify", results_limit=1)["vector"]

    self.assertEqual(len(items), 1)
    item = items[0]

    self.assertEqual(item.kind, PdfQueryKind.anno_content)
    self.assertEqual(item.pdf_hash, "8lMbEwyJykueeqwrQzrmFHt5usqlag47UpdUuFpXGfFptM13R3RUQ0AH0bCT93REjMbu25G43SFOduMehD_v8g==")
    self.assertEqual(item.page_index, 2)
    self.assertEqual(item.anno_index, 0)
    self.assertEqual(item.segment_start, 0)
    self.assertEqual(item.segment_end, len("Identification"))