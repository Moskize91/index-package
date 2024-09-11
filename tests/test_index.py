import os
import unittest

from index_package.parser import PdfParser
from index_package.scanner import Event, EventKind, EventTarget
from index_package.index import PdfQueryKind, VectorIndex
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_struct_and_destruct_pdf(self):
    parser = PdfParser(
      cache_path=get_temp_path("index/parser_cache"),
      temp_path=get_temp_path("index/temp"),
    )
    index = VectorIndex(
      parser=parser,
      db_path=get_temp_path("index/db"),
      embedding_model_id="shibing624/text2vec-base-chinese",
      scope_map={
        "assets": os.path.abspath(os.path.join(__file__, "../assets")),
      },
    )
    added_event = Event(
      id=0,
      kind=EventKind.Added,
      target=EventTarget.File,
      scope="test",
      path="/The Analysis of the Transference.pdf",
      mtime=0,
    )
    index.handle_event("assets", added_event)
    results = index.query(["identify"], results_limit=1)[0]

    self.assertEquals(len(results), 1)

    annotation = results[0]
    self.assertEquals(annotation.kind, PdfQueryKind.annotation_content)
    self.assertEquals(annotation.page_hash, "Ayy2i4OK41YmIejdNJYTfyl6SgC_7zd7q05vDUenDOBEmN3T6gtKTC5gP5a_-dxufdntkgR3f2agbwww5a3AsA==")
    self.assertEquals(annotation.pdf_hash, "8lMbEwyJykueeqwrQzrmFHt5usqlag47UpdUuFpXGfFptM13R3RUQ0AH0bCT93REjMbu25G43SFOduMehD_v8g==")
    self.assertEquals(annotation.index, 0)
    self.assertEquals(annotation.text, "Identification")