import os
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
      "Q9FWXE78o4aKMSv3t3LRJhJrJworxd3EJls6Dx1mOifWbIGmoS0CRf5Nx3t-ue_IqovVrrZy7ZUSkztABjqCCA==",
      "a7SZ-nxEp3tHjBgPq6ZOUKcCKuNlKwzt3GsMK4FGmCcYR8Q_CgshFKk3lAZY4sspgl-vd6T9sAGu6a2wXxtqBA==",
      "l02eglkFC4Yg2S7Gt44MuGne1PxnBgZ3lBgLvZ24GI0fwF-B70Sf4DjCxe_uU4KsZpyzKNasFLuxe_MUiSZXWQ==",
      "JAm_KnYWAmoZf1d4srzQSZnRn7UzUOYJy-phO8n33HVBL-37N1hAN0vXnQ7QlmFikuichCdYD4tM698KK8mbJQ==",
      "0INJs_zOQY96Qrn0JcKaGOiO5CF_SrclB54WKa4HJF171EPvubUAkcr2B8UJuKcTUfulA1gdDlnq4MCkf9fV_A==",
    ])

  def _assets_info(self, assets_path: str, name: str):
    path = os.path.join(assets_path, name)
    hash = hash_sha512(path)
    return path, hash