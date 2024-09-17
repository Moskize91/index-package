import os
import unittest

from index_package.parser import PdfParser
from index_package.scanner import Event, EventKind, EventTarget
from index_package.segmentation import Segmentation
from index_package.index2 import Index, VectorDB, FTS5DB, IndexNodeMatching
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_vector_index_for_pdf(self):
    segmentation = Segmentation()
    parser = PdfParser(
      cache_dir_path=get_temp_path("index_vector/parser_cache"),
      temp_dir_path=get_temp_path("index_vector/temp"),
    )
    fts5_db = FTS5DB(
      db_path=os.path.abspath(os.path.join(
        get_temp_path("index_fts5/fts5_db"),
        "db.sqlite3"
      )),
    )
    vector_db = VectorDB(
      index_dir_path=get_temp_path("index_vector/vector_db"),
      embedding_model_id="shibing624/text2vec-base-chinese",
    )
    index = Index(
      pdf_parser=parser,
      segmentation=segmentation,
      fts5_db=fts5_db,
      vector_db=vector_db,
      index_dir_path=get_temp_path("index_vector/index"),
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
    items = index.query("identify", results_limit=1)
    self.assertEqual(len(items), 1)
    item = items[0]
    self.assertEqual(item.id, "Ayy2i4OK41YmIejdNJYTfyl6SgC_7zd7q05vDUenDOBEmN3T6gtKTC5gP5a_-dxufdntkgR3f2agbwww5a3AsA==/anno/0/content")
    self.assertEqual(item.matching, IndexNodeMatching.Similarity)
    self.assertEqual(item.segments, [(0, len("Identification"))])