import os
import unittest

from typing import cast, Any
from chromadb.api.types import Documents, Embeddings, EmbeddingFunction
from sentence_transformers import SentenceTransformer

from index_package.parser import PdfParser
from index_package.scanner import Event, EventKind, EventTarget
from index_package.index import Index
from tests.utils import get_temp_path

class MyEmbeddingFunction(EmbeddingFunction):
  def __init__(self):
    self._model = SentenceTransformer("shibing624/text2vec-base-chinese")

  def __call__(self, input: Documents) -> Embeddings:
    return cast(Any, self._model.encode(input)).tolist()

class TestPdfParser(unittest.TestCase):

  def test_struct_and_destruct_pdf(self):
    parser = PdfParser(
      cache_path=get_temp_path("index/parser_cache"),
      temp_path=get_temp_path("index/temp"),
    )
    index = Index(
      parser=parser,
      db_path=get_temp_path("index/db"),
      embedding_function=MyEmbeddingFunction(),
      scope_map={
        "assets": os.path.abspath(os.path.join(__file__, "../assets")),
      },
    )
    added_event = Event(
      id=0,
      kind=EventKind.Added,
      target=EventTarget.File,
      path="/The Analysis of the Transference.pdf",
      mtime=0,
    )
    index.handle_event("assets", added_event)
    results = index.query("identify")

    for result in results:
      print("\n====================")
      print("distance:", result["distance"])
      print(result["document"])