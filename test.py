import unittest

loader = unittest.TestLoader()
suite = loader.discover("tests")
runner = unittest.TextTestRunner()
result = runner.run(suite)