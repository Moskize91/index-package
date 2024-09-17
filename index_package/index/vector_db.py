import re

from typing import cast, Any, Optional
from numpy import ndarray
from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import ID, EmbeddingFunction, IncludeEnum, Documents, Embeddings, Document, Metadata

from .types import IndexNode, IndexNodeMatching
from ..segmentation.segmentation import Segment

class VectorDB:
  def __init__(self, index_dir_path: str, embedding_model_id: str):
    chromadb: ClientAPI = PersistentClient(path=index_dir_path)
    self._db = chromadb.get_or_create_collection(
      name="nodes",
      embedding_function=_EmbeddingFunction(embedding_model_id),
    )

  def query(
    self,
    query_text: str,
    results_limit: int,
    matching: IndexNodeMatching = IndexNodeMatching.Similarity,
  ) -> list[IndexNode]:
    result = self._db.query(
      query_texts=[query_text],
      n_results=results_limit,
      include=[IncludeEnum.metadatas, IncludeEnum.distances],
    )
    ids = cast(list[list[ID]], result["ids"])[0]
    metadatas = cast(list[list[dict]], result["metadatas"])[0]
    distances = cast(list[list[float]], result["distances"])[0]
    node2segments: dict[str, list[tuple[float, int, int, dict]]] = {}

    for i in range(len(ids)):
      matches = re.match(r"(.*)/([^/]*)$", ids[i])
      if matches is None:
        raise ValueError(f"Invalid ID: {ids[i]}")
      node_id = matches.group(1)
      metadata: dict[str, Any] = metadatas[i]
      distance = distances[i]
      start = metadata.pop("seg_start")
      end = metadata.pop("seg_end")
      segments = node2segments.get(node_id, None)
      if segments is None:
        node2segments[node_id] = segments = []
      segments.append((distance, start, end, metadata))

    nodes: list[IndexNode] = []
    for node_id, segments in node2segments.items():
      node_segments: list[tuple[int, int]] = []
      node_metadata: Optional[dict] = None
      min_distance: float = float("inf")
      for distance, start, end, metadata in segments:
        node_segments.append((start, end))
        if node_metadata is None:
          node_metadata = metadata
        if distance < min_distance:
          min_distance = distance
      if node_metadata is None:
        continue
      nodes.append(IndexNode(
        id=node_id,
        matching=matching,
        metadata=node_metadata,
        rank=min_distance,
        segments=node_segments,
      ))
    nodes.sort(key=lambda node: node.rank)
    return nodes

  def save(self, node_id: str, segments: list[Segment], metadata: dict):
    ids: list[ID] = []
    documents: list[Document] = []
    metadatas: list[Metadata] = []

    for i, segment in enumerate(segments):
      segment_metadata = metadata.copy()
      segment_metadata["seg_start"] = segment.start
      segment_metadata["seg_end"] = segment.end
      ids.append(f"{node_id}/{i}")
      documents.append(segment.text)
      metadatas.append(segment_metadata)

    self._db.add(
      ids=ids,
      documents=documents,
      metadatas=metadatas,
    )

  def remove(self, node_id: str):
    offset: int = 0
    group_size: int = 25
    while True:
      ids = [f"{node_id}/{offset + i}" for i in range(group_size)]
      self._db.delete(ids=ids)
      offset += group_size
      id = f"{node_id}/{offset}"
      result = self._db.get(ids=id)
      if len(result.get("ids")) == 0:
        break

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