import os
import time
import shutil
import unittest

from index_package.scanner import Scanner, EventKind, EventSearcher, EventTarget
from tests.utils import get_temp_path

class TestScanner(unittest.TestCase):

  def test_scanning_folder(self):
    scan_path, db_path = self.setup_paths()
    scanner = Scanner(
      db_path=db_path,
      sources={ "test": scan_path },
    )
    self._test_insert_files(scan_path, scanner)
    time.sleep(0.1)
    self._test_modify_part_of_files(scan_path, scanner)
    time.sleep(0.1)
    self._test_delete_recursively(scan_path, scanner)

  def _test_insert_files(self, scan_path: str, scanner: Scanner):
    self.set_file(scan_path, "./foobar", "hello world")
    self.set_file(scan_path, "./earth/land", "this is a land")
    self.set_file(scan_path, "./earth/sea", "this is sea")
    self.set_file(scan_path, "./universe/sun/sun1", "this is sun1")
    self.set_file(scan_path, "./universe/sun/sun2", "this is sun1")
    self.set_file(scan_path, "./universe/moon/moon1", "this is moon1")

    with scanner.scan() as events:
      path_list: list[str] = []
      for event in events:
        path_list.append(event.path)
      path_list.sort()
      self.assertListEqual(path_list, [
        "/",
        "/earth", "/earth/land", "/earth/sea",
        "/foobar",
        "/universe", "/universe/moon", "/universe/moon/moon1",
        "/universe/sun", "/universe/sun/sun1", "/universe/sun/sun2",
      ])

  def _test_modify_part_of_files(self, scan_path: str, scanner: Scanner):
    self.set_file(scan_path, "./foobar", "file is foobar")
    self.set_file(scan_path, "./universe/moon/moon2", "this is moon2")
    self.set_file(scan_path, "./universe/moon/moon3", "this is moon3")
    self.del_file(scan_path, "./universe/sun/sun2")

    with scanner.scan() as events:
      added_path_list, removed_path_list, updated_path_list = self._classify_events(events)
      self.assertListEqual(added_path_list, [
        ("/universe/moon/moon2", EventTarget.File),
        ("/universe/moon/moon3", EventTarget.File),
      ])
      self.assertListEqual(updated_path_list, [
        ("/foobar", EventTarget.File),
        ("/universe/moon", EventTarget.Directory), # inserted files in it
        ("/universe/sun", EventTarget.Directory), # removed files in it
      ])
      self.assertListEqual(removed_path_list, [
        ("/universe/sun/sun2", EventTarget.File),
      ])

  def _test_delete_recursively(self, scan_path: str, scanner: Scanner):
    self.del_file(scan_path, "./universe")

    with scanner.scan() as events:
      added_path_list, removed_path_list, updated_path_list = self._classify_events(events)
      self.assertListEqual(added_path_list, [])
      self.assertListEqual(updated_path_list, [
        ("/", EventTarget.Directory), # removed files in it
      ])
      self.assertListEqual(removed_path_list, [
        ("/universe", EventTarget.Directory),
        ("/universe/moon", EventTarget.Directory),
        ("/universe/moon/moon1", EventTarget.File),
        ("/universe/moon/moon2", EventTarget.File),
        ("/universe/moon/moon3", EventTarget.File),
        ("/universe/sun", EventTarget.Directory),
        ("/universe/sun/sun1", EventTarget.File),
      ])

  def setup_paths(self) -> tuple[str, str]:
    temp_path = get_temp_path("scanner")
    scan_path = os.path.join(temp_path, "data")
    db_path = os.path.join(temp_path, "scanner.sqlite3")

    return scan_path, db_path

  def _classify_events(self, events: EventSearcher) -> tuple[
    list[tuple[str, EventTarget]],
    list[tuple[str, EventTarget]],
    list[tuple[str, EventTarget]],
  ]:
    added_path_list: list[tuple[str, EventTarget]] = []
    removed_path_list: list[tuple[str, EventTarget]] = []
    updated_path_list: list[tuple[str, EventTarget]] = []

    for event in events:
      if event.kind == EventKind.Added:
        added_path_list.append((event.path, event.target))
      elif event.kind == EventKind.Removed:
        removed_path_list.append((event.path, event.target))
      elif event.kind == EventKind.Updated:
        updated_path_list.append((event.path, event.target))

    for path_list in [added_path_list, removed_path_list, updated_path_list]:
      path_list.sort(key=lambda x: x[0])

    return added_path_list, removed_path_list, updated_path_list

  def set_file(self, base_path: str, path: str, content: str):
    abs_file_path = os.path.join(base_path, path)
    abs_dir_path = os.path.dirname(abs_file_path)
    os.makedirs(abs_dir_path, exist_ok=True)
    with open(abs_file_path, "w", encoding="utf-8") as file:
      file.write(content)

  def del_file(self, base_path: str, path: str):
    abs_file_path = os.path.join(base_path, path)
    if not os.path.exists(abs_file_path):
      return;
    if os.path.isfile(abs_file_path):
      os.remove(abs_file_path)
    elif os.path.isdir(abs_file_path):
      shutil.rmtree(abs_file_path)