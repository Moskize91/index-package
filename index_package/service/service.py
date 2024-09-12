import os

from typing import Optional, Union
from ..scanner import Scanner
from ..parser import PdfParser
from ..index import VectorIndex, PdfVectorResult
from ..utils import ensure_parent_dir

class Service:
  def __init__(
    self,
    workspace_path: str,
    embedding_model_id: str,
    sources: dict[str, str],
  ):
    self._sources: dict[str, str] = sources.copy()
    self._scanner: Scanner = Scanner(
      db_path=ensure_parent_dir(os.path.join(workspace_path, "scanner.sqlite3")),
      sources=self._sources,
    )
    self._index: VectorIndex = VectorIndex(
      root_dir_path=os.path.join(workspace_path, "indexes"),
      scope_map=self._sources,
      embedding_model_id=embedding_model_id,
      parser=PdfParser(
        cache_path=os.path.join(workspace_path, "parser", "cache", "pdf"),
        temp_path=os.path.join(workspace_path, "temp", "pdf_parser"),
      ),
    )

  def scan(self):
    with self._scanner.scan() as events:
      for event in events:
        self._index.handle_event(event)

  def query(self, texts: Union[list, list[str]], results_limit: Optional[int]) -> list[PdfVectorResult]:
    if isinstance(texts, str):
      texts = [texts]

    results: list[PdfVectorResult] = []

    for sub_results in self._index.query(texts, results_limit):
      for result in sub_results:
        results.append(result)

    results.sort(key=lambda result: result.distance)

    return results

  def files(self, hash: str) -> list[str]:
    return self._index.hash_to_files(hash)
