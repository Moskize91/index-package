import os
import time
import shutil
import unittest

from index_package.parser import PdfParser
from index_package.utils import hash_sha512
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_struct_and_destruct_pdf(self):
    assets_path = os.path.abspath(os.path.join(__file__, "../assets"))
    parser = PdfParser(
      cache_path=get_temp_path("pdf/cache"),
      temp_path=get_temp_path("pdf/temp"),
    )
    file1, file1_hash = self._assets_info(assets_path, "The Sublime Object of Ideology.pdf")
    file2, file2_hash = self._assets_info(assets_path, "铁证待判.pdf")

    parser.add_file(file1_hash, file1)
    file1_pages = [p.hash for p in parser.pages(file1_hash)]

    self.assertListEqual(file1_pages, [
      "bwUz-YCAgCmerMrITdL8h5wFm9NCqlDqKL_JK238MaS4zTGZ9InIUdKL5M1cwmHpkK_QnupzyfTIfD0UyqdNQA==",
      "Lo9DdycASHhdtCPPAQEltpVeYfxMb0fkBJFjzkpir2HEIJjC2d3QqT_E6G2UQWCYQ4rowI8C-zQOPC9XYksO7g==",
      "qRhojtmvrStVkLjopDGg3i1yI4Q7m8vTJ7KovbPt90A5O2akPvx-b_sURtGI1zhQr9il7v0PMqiowgknqMnqYQ==",
      "A0rjq4lbhMmTBxS79v1WbRDCYcLObwQkVoQG7Qa3GHUjsR6AqpyHpCs0RUPiqVoUs5h80mPomsevV_3vX6PHPQ==",
      "Ca1YFdCRX0MnpBIz1NCnVtGi5-YSTMmAGVN47msSBnmpmv0AtLI-VEbbsWQflsIp65PIq7Zt8bWz23jxKgB8dg==",
    ])

  def _assets_info(self, assets_path: str, name: str):
    path = os.path.join(assets_path, name)
    hash = hash_sha512(path)
    return path, hash