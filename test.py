import unittest

loader = unittest.TestLoader()
suite = loader.discover("tests")
# suite = loader.discover("tests", pattern="test_index.py")
runner = unittest.TextTestRunner()
result = runner.run(suite)