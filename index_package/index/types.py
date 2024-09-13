from dataclasses import dataclass
from enum import Enum

class PdfQueryKind(Enum):
  pdf = 1
  page = 2
  anno_content = 3
  anno_extracted = 4

@dataclass
class PdfQueryItem:
  kind: PdfQueryKind
  pdf_hash: str
  page_index: int
  anno_index: int
  segment_start: int
  segment_end: int
  rank: float