import os
import unittest

from index_package.parser import PdfParser, PdfPageUpdatedEvent
from index_package.utils import hash_sha512
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_struct_and_destruct_pdf(self):
    assets_path = os.path.abspath(os.path.join(__file__, "../assets"))
    parser = PdfParser(
      cache_path=get_temp_path("pdf_struct/cache"),
      temp_path=get_temp_path("pdf_struct/temp"),
    )
    common_page_hash = "l02eglkFC4Yg2S7Gt44MuGne1PxnBgZ3lBgLvZ24GI0fwF-B70Sf4DjCxe_uU4KsZpyzKNasFLuxe_MUiSZXWQ=="
    file1, file1_hash = self._assets_info(assets_path, "The Sublime Object of Ideology.pdf")
    file2, file2_hash = self._assets_info(assets_path, "铁证待判.pdf")
    event = parser.add_file(file1_hash, file1)
    added_page_hashes, removed_page_hashes = self._parse_event(event)

    self.assertListEqual(removed_page_hashes, [])
    self.assertListEqual(added_page_hashes, [
      "0INJs_zOQY96Qrn0JcKaGOiO5CF_SrclB54WKa4HJF171EPvubUAkcr2B8UJuKcTUfulA1gdDlnq4MCkf9fV_A==",
      "JAm_KnYWAmoZf1d4srzQSZnRn7UzUOYJy-phO8n33HVBL-37N1hAN0vXnQ7QlmFikuichCdYD4tM698KK8mbJQ==",
      "Q9FWXE78o4aKMSv3t3LRJhJrJworxd3EJls6Dx1mOifWbIGmoS0CRf5Nx3t-ue_IqovVrrZy7ZUSkztABjqCCA==",
      "a7SZ-nxEp3tHjBgPq6ZOUKcCKuNlKwzt3GsMK4FGmCcYR8Q_CgshFKk3lAZY4sspgl-vd6T9sAGu6a2wXxtqBA==",
      common_page_hash,
    ])

    file1_pages = [p.hash for p in parser.pages(file1_hash)]
    self.assertListEqual(file1_pages, [
      "Q9FWXE78o4aKMSv3t3LRJhJrJworxd3EJls6Dx1mOifWbIGmoS0CRf5Nx3t-ue_IqovVrrZy7ZUSkztABjqCCA==",
      "a7SZ-nxEp3tHjBgPq6ZOUKcCKuNlKwzt3GsMK4FGmCcYR8Q_CgshFKk3lAZY4sspgl-vd6T9sAGu6a2wXxtqBA==",
      common_page_hash,
      "JAm_KnYWAmoZf1d4srzQSZnRn7UzUOYJy-phO8n33HVBL-37N1hAN0vXnQ7QlmFikuichCdYD4tM698KK8mbJQ==",
      "0INJs_zOQY96Qrn0JcKaGOiO5CF_SrclB54WKa4HJF171EPvubUAkcr2B8UJuKcTUfulA1gdDlnq4MCkf9fV_A==",
    ])

    event = parser.add_file(file2_hash, file2)
    added_page_hashes, removed_page_hashes = self._parse_event(event)

    self.assertListEqual(removed_page_hashes, [])
    self.assertListEqual(added_page_hashes, [
      "-vvw3eNZ3OMZq0ncxk4ChrOarUUoAWWFsLAUFdUikKrNM_kod1K5aEEP4GmgrTxWudVSuKFM9mBoim-tuMvwyA==",
      "Ia45CBC7oH2QMFlqoBny9RM6dVC2EDgMmLnz1qwesgT5tHsTxDqDg-1CdTeseZPJ-IGYOvDSw8pd7hnbyXGN_Q==",
      "imR7_eEgq9RAcTRjrnWaQBhr4F835Sqy861nX7SgO1MXDNn1n71vK0zz_MIi5pgfx9v8d4yZY8oyHfOR4O0iqQ==",
      "n2ce9j2Z36PeINfVHZnYYLmgk-h7wA91Ft8TCFs9UbE72BiO_KC3QtQ41hRmuyQvosnSVlUbt13im5A2RO818A==",
    ])

    file2_pages = [p.hash for p in parser.pages(file2_hash)]

    self.assertListEqual(file2_pages, [
      "n2ce9j2Z36PeINfVHZnYYLmgk-h7wA91Ft8TCFs9UbE72BiO_KC3QtQ41hRmuyQvosnSVlUbt13im5A2RO818A==",
      "Ia45CBC7oH2QMFlqoBny9RM6dVC2EDgMmLnz1qwesgT5tHsTxDqDg-1CdTeseZPJ-IGYOvDSw8pd7hnbyXGN_Q==",
      common_page_hash,
      "-vvw3eNZ3OMZq0ncxk4ChrOarUUoAWWFsLAUFdUikKrNM_kod1K5aEEP4GmgrTxWudVSuKFM9mBoim-tuMvwyA==",
      "imR7_eEgq9RAcTRjrnWaQBhr4F835Sqy861nX7SgO1MXDNn1n71vK0zz_MIi5pgfx9v8d4yZY8oyHfOR4O0iqQ==",
    ])
    hash_of_pdf: list[str] = list(set(file1_pages + file2_pages))
    hash_of_pdf.sort()

    self.assertListEqual(
      hash_of_pdf,
      self._read_hash_of_files(parser._pages_path),
    )
    event = parser.remove_file(file2_hash)
    added_page_hashes, removed_page_hashes = self._parse_event(event)

    self.assertListEqual(added_page_hashes, [])
    self.assertListEqual(removed_page_hashes, [
      "-vvw3eNZ3OMZq0ncxk4ChrOarUUoAWWFsLAUFdUikKrNM_kod1K5aEEP4GmgrTxWudVSuKFM9mBoim-tuMvwyA==",
      "Ia45CBC7oH2QMFlqoBny9RM6dVC2EDgMmLnz1qwesgT5tHsTxDqDg-1CdTeseZPJ-IGYOvDSw8pd7hnbyXGN_Q==",
      "imR7_eEgq9RAcTRjrnWaQBhr4F835Sqy861nX7SgO1MXDNn1n71vK0zz_MIi5pgfx9v8d4yZY8oyHfOR4O0iqQ==",
      "n2ce9j2Z36PeINfVHZnYYLmgk-h7wA91Ft8TCFs9UbE72BiO_KC3QtQ41hRmuyQvosnSVlUbt13im5A2RO818A==",
    ])

    self.assertListEqual([], parser.pages(file2_hash))

    hash_of_pdf = list(set(file1_pages))
    hash_of_pdf.sort()
    self.assertListEqual(
      hash_of_pdf,
      self._read_hash_of_files(parser._pages_path),
    )
    event = parser.remove_file(file1_hash)
    added_page_hashes, removed_page_hashes = self._parse_event(event)

    self.assertListEqual(added_page_hashes, [])
    self.assertListEqual(removed_page_hashes, [
      "0INJs_zOQY96Qrn0JcKaGOiO5CF_SrclB54WKa4HJF171EPvubUAkcr2B8UJuKcTUfulA1gdDlnq4MCkf9fV_A==",
      "JAm_KnYWAmoZf1d4srzQSZnRn7UzUOYJy-phO8n33HVBL-37N1hAN0vXnQ7QlmFikuichCdYD4tM698KK8mbJQ==",
      "Q9FWXE78o4aKMSv3t3LRJhJrJworxd3EJls6Dx1mOifWbIGmoS0CRf5Nx3t-ue_IqovVrrZy7ZUSkztABjqCCA==",
      "a7SZ-nxEp3tHjBgPq6ZOUKcCKuNlKwzt3GsMK4FGmCcYR8Q_CgshFKk3lAZY4sspgl-vd6T9sAGu6a2wXxtqBA==",
      common_page_hash,
    ])

    self.assertListEqual([], parser.pages(file1_hash))
    self.assertListEqual(
      [],
      self._read_hash_of_files(parser._pages_path),
    )

  def test_extract_annotation(self):
    assets_path = os.path.abspath(os.path.join(__file__, "../assets"))
    parser = PdfParser(
      cache_path=get_temp_path("pdf_extract/cache"),
      temp_path=get_temp_path("pdf_extract/temp"),
    )
    file, file_hash = self._assets_info(assets_path, "纯粹理性批判.pdf")
    event = parser.add_file(file_hash, file)
    added_page_hashes, removed_page_hashes = self._parse_event(event)

    self.assertListEqual(removed_page_hashes, [])
    self.assertListEqual(added_page_hashes, [
      "FOFSBh_4b2kLVr_gLT11cM42DEq01yposvIRaiOdw4Woki0FeD9mVp2l7SVdWu_R7tDV1ZbFOKTF8RE8wzv61w==",
      "mSmFG7L5wWaPNS2xfNJwyybeouZE1RwfF7sqmhFshVd6G137gapjXCm2hz1PtxKhIqOAKQ6xV61UsD2xortrRA==",
      "wuz6VCqK6qpvm9kkF5sU7IWc343pdAX4Cm_xcbrs-nxiNFvE4NFyXjZhBY0AP8iWp2ZPWEymk0iwzES8GnUNwQ==",
    ])

  def _assets_info(self, assets_path: str, name: str):
    path = os.path.join(assets_path, name)
    hash = hash_sha512(path)
    return path, hash

  def _read_hash_of_files(self, dir_path: str) -> list[str]:
    hash_of_files: list[str] = []

    for file_name in os.listdir(dir_path):
      hash, ext_name = os.path.splitext(file_name)
      if ext_name == ".pdf":
        hash_of_files.append(hash)

    hash_of_files.sort()
    return hash_of_files

  def _parse_event(self, event: PdfPageUpdatedEvent):
    event.added_page_hashes.sort()
    event.removed_page_hashes.sort()
    return event.added_page_hashes, event.removed_page_hashes