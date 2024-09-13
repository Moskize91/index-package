import unittest

try:
  loader = unittest.TestLoader()
  # suite = loader.discover("tests")
  suite = loader.discover("tests", pattern="test_pdf_parser.py")
  runner = unittest.TextTestRunner()
  result = runner.run(suite)
  if not result.wasSuccessful():
    exit(1)
except Exception as e:
  print(e)
  exit(1)