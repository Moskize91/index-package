from typing import cast, Optional
from numpy import ndarray
from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import EmbeddingFunction, ID, Documents, Embeddings, Document

from .abc_db import IndexDB, IndexItem
from ..utils import ensure_parent_dir

class VectorDB(IndexDB):
  def __init__(self, index_dir_path: str, embedding_model_id: str) -> None:
    super().__init__("vector")
    self._chromadb: ClientAPI = PersistentClient(
      path=ensure_parent_dir(index_dir_path)
    )
    self._pdfs_db = self._chromadb.get_or_create_collection(
      name="pdfs",
      embedding_function=_EmbeddingFunction(embedding_model_id),
    )

  def save_index(self, id: str, document: str, metadata: dict):
    self._pdfs_db.add(
      ids=id,
      documents=document,
      metadatas=metadata,
    )

  def remove_index(self, prefix_id: str):
    offset: int = 0
    group_size: int = 25
    while True:
      ids = [f"{prefix_id}/{offset + i}" for i in range(group_size)]
      self._pdfs_db.delete(ids=ids)
      offset += group_size
      id = f"{prefix_id}/{offset}"
      result = self._pdfs_db.get(ids=id)
      if len(result.get("ids")) == 0:
        break

  def query(self, keywords: list[str], results_limit: int) -> list[list[IndexItem]]:
    result = self._pdfs_db.query(
      query_texts=keywords,
      n_results=results_limit,
    )
    ids = cast(list[list[ID]], result.get("ids", []))
    documents = cast(list[list[Document]], result.get("documents", []))
    metadatas = cast(list[list[dict]], result.get("metadatas", []))
    distances = cast(list[list[float]], result.get("distances", []))
    results: list[list[IndexItem]] = []

    for i in range(len(documents)):
      j_results: list[IndexItem] = []
      results.append(j_results)
      sub_ids = ids[i]
      sub_documents = documents[i]
      sub_metadatas = metadatas[i]
      sub_distances = distances[i]
      for j in range(len(sub_ids)):
        j_results.append(IndexItem(
          id=sub_ids[j],
          document=sub_documents[j],
          metadata=sub_metadatas[j],
          rank=float(sub_distances[j]),
        ))

    return results

class _EmbeddingFunction(EmbeddingFunction):
  def __init__(self, model_id: str):
    self._model_id: str = model_id
    self._model: Optional[SentenceTransformer] = None

  def __call__(self, input: Documents) -> Embeddings:
    if self._model is None:
      self._model = SentenceTransformer(self._model_id)
    result = self._model.encode(input)
    if not isinstance(result, ndarray):
      raise ValueError("Model output is not a numpy array")
    return result.tolist()
