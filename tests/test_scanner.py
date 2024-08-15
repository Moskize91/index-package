import os
import shutil
import unittest

from index_package import scan

class TestSession(unittest.TestCase):

  def test_scanning_folder(self):
    temp_path = os.path.abspath(os.path.join(__file__, "../test_temp"))

    if os.path.exists(temp_path):
      shutil.rmtree(temp_path)

    scan_path = os.path.join(temp_path, "data")
    db_path = os.path.join(temp_path, "index.db")

    set_file(scan_path, "./foobar", "hello world")
    set_file(scan_path, "./earth/land", "this is a land")
    set_file(scan_path, "./earth/sea", "this is sea")
    set_file(scan_path, "./universe/sun/sun1", "this is sun1")
    set_file(scan_path, "./universe/sun/sun2", "this is sun1")
    set_file(scan_path, "./universe/moon/moon1", "this is moon1")

    with scan(scan_path, db_path) as events:
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

def set_file(base_path: str, path: str, content: str):
  abs_file_path = os.path.join(base_path, path)
  abs_dir_path = os.path.dirname(abs_file_path)
  os.makedirs(abs_dir_path, exist_ok=True)
  with open(abs_file_path, "w", encoding="utf-8") as file:
    file.write(content)