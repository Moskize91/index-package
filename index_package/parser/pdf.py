import os
import shutil
import sqlite3
import pikepdf

from .pdf_extractor import PdfExtractor
from ..utils import hash_sha512, TempFolderHub

class PdfPage:
  def __init__(self, parent, hash: str):
    self.hash: str= hash
    self._parent = parent

  def path(self) -> str:
    return os.path.join(self._parent._pages_path, f"{self.hash}.pdf")

# https://pikepdf.readthedocs.io/en/latest/
class PdfParser:
  def __init__(self, cache_path: str, temp_path: str) -> None:
    self._pages_path: str = os.path.join(cache_path, "pages")
    self._temp_folders: TempFolderHub = TempFolderHub(temp_path)
    self._conn: sqlite3.Connection = self._connect(os.path.join(cache_path, "pages.db"))
    self._cursor: sqlite3.Cursor = self._conn.cursor()
    self._extractor: PdfExtractor = PdfExtractor(self._pages_path)

    if not os.path.exists(self._pages_path):
      os.makedirs(self._pages_path, exist_ok=True)

  def pages(self, hash: str) -> list[PdfPage]:
    pdf_pages: list[PdfPage] = []
    for page_hash in self._select_page_hashes(hash):
      pdf_page = PdfPage(self, page_hash)
      pdf_pages.append(pdf_page)
    return pdf_pages

  def add_file(self, hash: str, file_path: str):
    origin_page_hashes = self._select_page_hashes(hash)
    new_page_hashes = self._extract_page_hashes(file_path)
    added_page_hashes, removed_page_hashes = self._update_db(origin_page_hashes, new_page_hashes, hash)

    for page_hash in added_page_hashes:
      self._extractor.extract_page(page_hash)

    for page_hash in removed_page_hashes:
      self._extractor.remove_page(page_hash)

  def remove_file(self, hash: str):
    page_hashes = self._select_page_hashes(hash)
    try:
      self._cursor.execute("BEGIN TRANSACTION")
      self._cursor.execute("DELETE FROM pages WHERE pdf_hash = ?", (hash,))
      self._conn.commit()
    except Exception as e:
      self._conn.rollback()
      raise e
    for page_hash in page_hashes:
      self._cursor.execute("SELECT * FROM pages WHERE page_hash = ?", (page_hash,))
      if self._cursor.fetchone() is None:
        self._extractor.remove_page(page_hash)

  def _select_page_hashes(self, hash: str) -> list[str]:
    self._cursor.execute(
      "SELECT page_index, page_hash FROM pages WHERE pdf_hash = ? ORDER BY page_index",
      (hash,),
    )
    page_hash_list: list[str] = []

    for row in self._cursor.fetchall():
      _, page_hash = row
      page_hash_list.append(page_hash)

    return page_hash_list

  def _extract_page_hashes(self, file_path: str) -> list[str]:
    page_hashes: list[str] = []

    with self._temp_folders.create() as folder:
      folder_path = folder.path
      pages_count: int = 0

      with pikepdf.Pdf.open(file_path) as pdf_file:
        for i, page in enumerate(pdf_file.pages):
          page_file = pikepdf.Pdf.new()
          page_file.pages.append(page)
          page_file_path = os.path.join(folder_path, f"{i}.pdf")
          page_file.save(
            page_file_path,
            # make sure hash of file never changes
            deterministic_id=True,
          )
        pages_count = len(pdf_file.pages)

      for i in range(pages_count):
        page_file_path = os.path.join(folder_path, f"{i}.pdf")
        page_hash = hash_sha512(page_file_path)
        page_hashes.append(page_hash)
        target_page_path = os.path.join(self._pages_path, f"{page_hash}.pdf")

        if not os.path.exists(target_page_path):
          shutil.move(page_file_path, target_page_path)
        elif os.path.isdir(target_page_path):
          shutil.rmtree(target_page_path)
          shutil.move(page_file_path, target_page_path)

    return page_hashes

  def _update_db(
    self,
    origin_page_hashes: list[str],
    new_page_hashes: list[str],
    pdf_hash: str,
  ) -> tuple[list[str], list[str]]:

    to_remove_hashes = set(origin_page_hashes)
    to_add_hashes = set(new_page_hashes)

    for hash in new_page_hashes:
      to_remove_hashes.discard(hash)

    for hash in origin_page_hashes:
      to_add_hashes.discard(hash)

    try:
      self._cursor.execute("BEGIN TRANSACTION")
      self._cursor.execute("DELETE FROM pages WHERE pdf_hash = ?", (pdf_hash,))

      for i, page_hash in enumerate(new_page_hashes):
        self._cursor.execute(
          "INSERT INTO pages (pdf_hash, page_index, page_hash) VALUES (?, ?, ?)",
          (pdf_hash, i, page_hash),
        )
      removed_hashes: list[str] = []
      added_hashes: list[str] = []

      for to_remove_hash in to_remove_hashes:
        self._cursor.execute("SELECT * FROM pages WHERE page_hash = ?", (to_remove_hash,))
        if self._cursor.fetchone() is None:
          removed_hashes.append(to_remove_hash)

      for to_add_hash in to_add_hashes:
        self._cursor.execute("SELECT COUNT(*) FROM pages WHERE page_hash = ?", (to_add_hash,))
        num_rows = self._cursor.fetchone()[0]
        if num_rows == 1:
          added_hashes.append(to_add_hash)

      self._conn.commit()
      return added_hashes, removed_hashes

    except Exception as e:
      self._conn.rollback()
      raise e

  def _connect(self, db_path: str) -> sqlite3.Connection:
    is_first_time = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    os.path.getmtime(db_path)

    if is_first_time:
      cursor = conn.cursor()
      cursor.execute("""
        CREATE TABLE pages (
          id INTEGER PRIMARY KEY,
          pdf_hash TEXT,
          page_index TEXT,
          page_hash TEXT
        )
      """)
      cursor.execute("""
        CREATE UNIQUE INDEX idx_pdf_pages ON pages (pdf_hash, page_index)
      """)
      cursor.execute("""
        CREATE INDEX idx_page_pages ON pages (page_hash)
      """)
      conn.commit()
      cursor.close()

    return conn