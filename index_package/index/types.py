from dataclasses import dataclass
from enum import Enum

class IndexNodeMatching(Enum):
  Matched = "matched"
  MatchedPartial = "matched_partial"
  Similarity = "similarity"

@dataclass
class IndexNode:
  id: str
  matching: IndexNodeMatching
  metadata: dict
  rank: float
  segments: list[tuple[int, int]]

@dataclass
class PageRelativeToPDF:
  pdf_hash: str
  pdf_path: str
  page_index: int