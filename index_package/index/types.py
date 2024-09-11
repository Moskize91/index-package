from dataclasses import dataclass
from enum import Enum

class PdfQueryKind(Enum):
  page = 1
  annotation_content = 2
  annotation_extracted = 3

@dataclass
class PdfVectorResult:
  kind: PdfQueryKind
  page_hash: str
  pdf_hash: str
  index: int
  text: str
  distance: float