import hashlib

def hash_sha512(file_path, chunk_size: int = 4096):
  sha512_hash = hashlib.sha512()
  with open(file_path, "rb") as file:
    while chunk := file.read(chunk_size):
      sha512_hash.update(chunk)
  return sha512_hash.hexdigest()