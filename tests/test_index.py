import os
import unittest

from index_package.parser import PdfParser
from index_package.scanner import Event, EventKind, EventTarget
from index_package.segmentation import Segmentation
from index_package.index import Index, VectorDB, PdfQueryKind
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_struct_and_destruct_pdf(self):
    segmentation = Segmentation()
    parser = PdfParser(
      cache_dir_path=get_temp_path("index/parser_cache"),
      temp_dir_path=get_temp_path("index/temp"),
    )
    vector_db = VectorDB(
      index_dir_path=get_temp_path("index/vector_db"),
      embedding_model_id="shibing624/text2vec-base-chinese",
    )
    index = Index(
      pdf_parser=parser,
      segmentation=segmentation,
      index_dir_path=get_temp_path("index/index"),
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

    self.assertEquals(len(items), 1)
    item = items[0]

    self.assertEquals(item.kind, PdfQueryKind.anno_content)
    self.assertEquals(item.pdf_hash, "8lMbEwyJykueeqwrQzrmFHt5usqlag47UpdUuFpXGfFptM13R3RUQ0AH0bCT93REjMbu25G43SFOduMehD_v8g==")
    self.assertEquals(item.page_index, 2)
    self.assertEquals(item.anno_index, 0)
    self.assertEquals(item.segment_start, 0)
    self.assertEquals(item.segment_end, 1) # TODO: 这是错的，需要修复