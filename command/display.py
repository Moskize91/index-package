from index_package import Service, PdfQueryKind, PdfQueryItem


def show_items(text: list[str], service: Service, items: list[PdfQueryItem]):
  # command will see the bottom item first
  items.reverse()

  for item in items:
    print("========================================")
    if item.kind == PdfQueryKind.pdf:
      print(f"PDF Metadata")
    elif item.kind == PdfQueryKind.page:
      print(f"PDF page at page {item.page_index + 1}")
    elif item.kind == PdfQueryKind.anno_content:
      print(f"Annotation Content at Page {item.page_index + 1}")
    elif item.kind == PdfQueryKind.anno_extracted:
      print(f"Annotation Extracted Text at Page {item.page_index + 1}")
    print(f"Rank: {item.rank}")

    files = service.get_paths(item.pdf_hash)

    if len(files) > 0:
      print("Files:")
      for file in files:
        print(f"  {file}")

    print("----------------------------------------")
    print(service.page_content(item.pdf_hash, item.page_index))
    print("")

  query_text = ", ".join(text)

  print(f"Query: {query_text}")
  print(f"Found {len(items)} results")