import os
import re
import pdfplumber
import json

from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from pdfplumber.page import Page
from shapely.geometry import Polygon

_PDF_EXT = "pdf"
_SNAPSHOT_EXT = "snapshot.txt"
_ANNOTATION_EXT = "annotation.json"

@dataclass
class _Annotation:
  type: Optional[str]
  title: Optional[str]
  content: Optional[str]
  uri: Optional[str]
  created_at: Optional[str]
  updated_at: Optional[str]
  quad_points: Optional[list[float]]
  extracted_text: Optional[str]

class _AnnotationPolygon:
  def __init__(self, quad_points: list[float]):
    self._polygons: list[Polygon] = []
    for i in range(int(len(quad_points) / 8)):
      x0 = float("inf")
      x1 = -float("inf")
      y0 = float("inf")
      y1 = -float("inf")
      for j in range(4):
        index = i*8 + j*2
        x = quad_points[index]
        y = quad_points[index + 1]
        x0 = min(x0, x)
        x1 = max(x1, x)
        y0 = min(y0, y)
        y1 = max(y1, y)
      polygon = Polygon(((x0, y0), (x1, y0), (x1, y1), (x0, y1)))
      if polygon.is_valid:
        self._polygons.append(polygon)

  @property
  def is_valid(self) -> bool:
    return len(self._polygons) > 0

  def intersects(self, x0: float, y0: float, x1: float, y1: float) -> bool:
    target_polygon = Polygon(((x0, y0), (x1, y0), (x1, y1), (x0, y1)))
    for polygon in self._polygons:
      if polygon.overlaps(target_polygon):
        return True
    return False

  def contains(self, x0: float, y0: float, x1: float, y1: float) -> bool:
    # make target smaller to be contained
    rate = 0.01
    center_x = (x0 + x1) / 2.0
    center_y = (y0 + y1) / 2.0
    x0 += (center_x - x0) * rate
    y0 += (center_y - y0) * rate
    x1 += (center_x - x1) * rate
    y1 += (center_y - y1) * rate
    target_polygon = Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
    for polygon in self._polygons:
      if polygon.contains(target_polygon):
        return True
    return False

class PdfExtractor:
  def __init__(self, pages_path: str):
    self._pages_path: str = pages_path

  def extract_page(self, page_hash: str):
    global _PDF_EXT, _SNAPSHOT_EXT, _ANNOTATION_EXT
    annotations: list[_Annotation]
    snapshot: str = ""

    with pdfplumber.open(os.path.join(self._pages_path, f"{page_hash}.{_PDF_EXT}")) as pdf_file:
      if len(pdf_file.pages) == 0:
        return
      page = pdf_file.pages[0]
      annotations = self._extract_annotations(page)
      snapshot = page.extract_text_simple()

      for annotation in annotations:
        quad_points = annotation.quad_points
        if quad_points is not None:
          annotation.extracted_text = self._extract_selected_text(page, quad_points)

    if not self._is_all_whitespace(snapshot):
      with open(os.path.join(self._pages_path, f"{page_hash}.{_SNAPSHOT_EXT}"), "w", encoding="utf-8") as file:
        file.write(snapshot)

    if len(annotations) > 0:
      with open(os.path.join(self._pages_path, f"{page_hash}.{_ANNOTATION_EXT}"), 'w', encoding='utf-8') as file:
        annotation_json: list[dict] = []
        for annotation in annotations:
          to_json = {}
          annotation_json.append(to_json)
          if annotation.type is not None:
            to_json["type"] = annotation.type
          if annotation.title is not None:
            to_json["title"] = annotation.title
          if annotation.content is not None:
            to_json["content"] = annotation.content
          if annotation.uri is not None:
            to_json["uri"] = annotation.uri
          if annotation.created_at is not None:
            to_json["createdAt"] = annotation.created_at
          if annotation.updated_at is not None:
            to_json["updatedAt"] = annotation.updated_at
          if annotation.quad_points is not None:
            to_json["quadPoints"] = annotation.quad_points
          if annotation.extracted_text is not None:
            to_json["extractedText"] = annotation.extracted_text

        json.dump(annotation_json, file, ensure_ascii=False)

  def remove_page(self, page_hash: str):
    global _PDF_EXT, _SNAPSHOT_EXT, _ANNOTATION_EXT
    for ext_name in (_PDF_EXT, _SNAPSHOT_EXT, _ANNOTATION_EXT):
      file_path = os.path.join(self._pages_path, f"{page_hash}.{ext_name}")
      if os.path.exists(file_path):
        os.remove(file_path)

  def _extract_annotations(self, page: Page) -> list[_Annotation]:
    annotations: list[_Annotation] = []
    for anno in page.annots:
      if anno.get("object_type", "") != "annot":
        continue
      annotation = _Annotation(
        type=None,
        title=anno.get("title", None),
        content=anno.get("contents", None),
        uri=anno.get("uri", None),
        created_at=None,
        updated_at=None,
        quad_points=None,
        extracted_text=None,
      )
      data = anno.get("data", None)
      if data is not None:
        annotation.quad_points = data.get("QuadPoints", None)
        sub_type = data.get("Subtype", None)

        if sub_type is not None:
          annotation.type = sub_type.name

        creation_date = data.get("CreationDate", None)
        updated_date = data.get("M", None)

        if creation_date is not None:
          annotation.created_at = self._convert_to_utc(creation_date.decode("utf-8"))
        if updated_date is not None:
          annotation.updated_at = self._convert_to_utc(updated_date.decode("utf-8"))

      if annotation.title is not None or \
          annotation.content is not None or \
          annotation.uri is not None:
        annotations.append(annotation)
    return annotations

  def _extract_selected_text(self, page: Page, quad_points: list[float]) -> Optional[str]:
    annotation_polygon = _AnnotationPolygon(quad_points)
    if not annotation_polygon.is_valid:
      return None

    line_tuples: list[tuple[float, str]] = []
    height = page.height

    for line in page.extract_text_lines(char=False):
      x0, y0, x1, y1 = line["x0"], line["top"], line["x1"], line["bottom"]
      # coordinate system is from bottom-left
      y0 = height - y0
      y1 = height - y1
      if not annotation_polygon.intersects(x0, y0, x1, y1):
        continue

      line_chars: list[str] = []
      for char in line["chars"]:
        x0, y0, x1, y1 = char["x0"], char["y0"], char["x1"], char["y1"]
        if annotation_polygon.contains(x0, y0, x1, y1):
          line_chars.append(char["text"])
      line_tuples.append((-y0, "".join(line_chars)))

    lines: list[str] = []
    for (_, line) in sorted(line_tuples, key=lambda x: x[0]):
      lines.append(line)

    if len(lines) == 0:
      return None
    else:
      return "\n".join(lines)

  def _convert_to_utc(self, timestamp: str):
    pattern = r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})([\+\-]\d{2})'(\d{2})"
    match = re.match(pattern, timestamp)
    if match:
        year, month, day, hour, minute, second, timezone_offset_hour, timezone_offset_minute = match.groups()
        dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        utc_offset = timedelta(hours=int(timezone_offset_hour), minutes=int(timezone_offset_minute))
        dt_adjusted = dt - utc_offset
        return dt_adjusted.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return None

  def _is_all_whitespace(self, string: str):
    return all(c.isspace() for c in string)