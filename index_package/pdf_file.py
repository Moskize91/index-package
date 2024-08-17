from typing import Optional
from index_package.chunk import ChunkHub

class PDFFile:
  def __init__(
    self,
    uid: str,
    hash: str,
    title: str,
    author: str,
  ):
    self._uid: str = uid
    self._hash: str = hash
    self._title: str = title
    self._author: str = author

  @property
  def hash(self) -> str:
    return self._hash

  @property
  def title(self) -> str:
    return self._title

  @property
  def author(self) -> str:
    return self._author

class PDFFileHub:
  def __init__(self, chunks: ChunkHub, client):
    self._chunks: ChunkHub = chunks
    self._client = client