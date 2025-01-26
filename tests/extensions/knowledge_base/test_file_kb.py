import os
import time
import shutil
import unittest

from typing import Generator, Callable
from index_package.extensions.knowledge_base.file import FileKnowledgeBase
from index_package.extensions.knowledge_base import Event, EventKind
from tests.utils import get_temp_path


class _Context:
  def assert_continue(self):
    pass

class TestFileKB(unittest.TestCase):

  def test_file_knowledge_base(self):
    scan_path, db_path = self._setup_paths()
    kb = FileKnowledgeBase(db_path)
    kb.put_scope("test", scan_path)

    self._test_insert_files(scan_path, kb)
    time.sleep(0.1)
    self._test_modify_part_of_files(scan_path, kb)
    time.sleep(0.1)
    self._test_delete_recursively(scan_path, kb)

  def _test_insert_files(self, scan_path: str, kb: FileKnowledgeBase):
    self._set_file(scan_path, "./foobar", "hello world")
    self._set_file(scan_path, "./earth/land", "this is a land")
    self._set_file(scan_path, "./earth/sea", "this is sea")
    self._set_file(scan_path, "./universe/sun/sun1", "this is sun1")
    self._set_file(scan_path, "./universe/sun/sun2", "this is sun1")
    self._set_file(scan_path, "./universe/moon/moon1", "this is moon1")
    path_list: list[str] = []

    for event, dispose in self._scan(kb):
      try:
        path_list.append(event.id)
      finally:
        dispose()

    path_list.sort()
    self.assertListEqual(path_list, [
      "test/foobar",
      "test/earth/land", "/earth/sea",
      "test/universe/sun/sun1", "/universe/sun/sun2",
      "test/universe/moon/moon1",
    ])

  def _test_modify_part_of_files(self, scan_path: str, kb: FileKnowledgeBase):
    self._set_file(scan_path, "./foobar", "file is foobar")
    self._set_file(scan_path, "./universe/moon/moon2", "this is moon2")
    self._set_file(scan_path, "./universe/moon/moon3", "this is moon3")
    self._del_file(scan_path, "./universe/sun/sun2")

    added_path_list: list[str] = []
    removed_path_list: list[str] = []
    updated_path_list: list[str] = []

    for event, dispose in self._scan(kb):
      try:
        if event.kind == EventKind.Added:
          added_path_list.append(event.id)
        elif event.kind == EventKind.Removed:
          removed_path_list.append(event.id)
        elif event.kind == EventKind.Updated:
          updated_path_list.append(event.id)
      finally:
        dispose()

    self.assertListEqual(added_path_list, [
      "test/universe/moon/moon2",
      "test/universe/moon/moon3",
    ])
    self.assertListEqual(updated_path_list, [
      "test/foobar",
    ])
    self.assertListEqual(removed_path_list, [
      "test/universe/sun/sun2",
    ])

  def _test_delete_recursively(self, scan_path: str, kb: FileKnowledgeBase):
    self._del_file(scan_path, "./universe")
    added_path_list: list[str] = []
    removed_path_list: list[str] = []
    updated_path_list: list[str] = []

    for event, dispose in self._scan(kb):
      try:
        if event.kind == EventKind.Added:
          added_path_list.append(event.id)
        elif event.kind == EventKind.Removed:
          removed_path_list.append(event.id)
        elif event.kind == EventKind.Updated:
          updated_path_list.append(event.id)
      finally:
        dispose()

    self.assertListEqual(added_path_list, [])
    self.assertListEqual(updated_path_list, [])
    self.assertListEqual(removed_path_list, [
      "test/universe/moon/moon1",
      "test/universe/moon/moon2",
      "test/universe/moon/moon3",
      "test/universe/sun/sun1",
    ])

  def _setup_paths(self) -> tuple[str, str]:
    temp_path = get_temp_path("file-knowledge-base")
    scan_path = os.path.join(temp_path, "data")
    db_path = os.path.join(temp_path, "file.sqlite3")

    return scan_path, db_path

  def _set_file(self, base_path: str, path: str, content: str):
    abs_file_path = os.path.join(base_path, path)
    abs_dir_path = os.path.dirname(abs_file_path)
    os.makedirs(abs_dir_path, exist_ok=True)
    with open(abs_file_path, "w", encoding="utf-8") as file:
      file.write(content)

  def _del_file(self, base_path: str, path: str):
    abs_file_path = os.path.join(base_path, path)
    if not os.path.exists(abs_file_path):
      return
    if os.path.isfile(abs_file_path):
      os.remove(abs_file_path)
    elif os.path.isdir(abs_file_path):
      shutil.rmtree(abs_file_path)

  def _scan(self, kb: FileKnowledgeBase, kind: EventKind | None = None) -> Generator[tuple[Event, Callable[[], None]], None, None]:
    extension = kb.create_extension()
    extension.scan(_Context())
    while True:
      event = extension.oldest_event(kind)
      if event is None:
        break
      yield event, lambda:extension.remove_event(event)