import os
import shutil
import unittest

def setup_test_directory():
  temp_path = os.path.abspath(os.path.join(__file__, "../tests/test_temp"))
  if os.path.exists(temp_path):
    shutil.rmtree(temp_path)
  os.makedirs(temp_path)

loader = unittest.TestLoader()
suite = loader.discover("tests")

setup_test_directory()

runner = unittest.TextTestRunner()
result = runner.run(suite)