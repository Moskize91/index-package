import os
import sqlite3

def main():
  path = "./fts5/fts5.sqlite3"
  if os.path.exists(path):
    os.remove(path)
  conn = sqlite3.connect(path)
  # conn.enable_load_extension(True)
  # conn.load_extension("fts5")

  conn.execute("""create virtual table fts5test using fts5 (data);""")
  conn.execute("""insert into fts5test (data)
                  values ('this is a test of full-text search');""")
  rows = conn.execute("""select rowid, data from fts5test where data match 'full';""").fetchall()
  for row in rows:
    print(row)

if __name__ == "__main__":
  main()