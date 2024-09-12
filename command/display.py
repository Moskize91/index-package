from index_package import Service, PdfVectorResult, PdfQueryKind


def show_results(service: Service, results: list[PdfVectorResult]):
  print(f"Found {len(results)} results")
  print("")

  # command will see the bottom item first
  results.reverse()

  for result in results:
    print("========================================")
    if result.kind == PdfQueryKind.page:
      print(f"PDF page at page {result.index + 1}")
    elif result.kind == PdfQueryKind.annotation_content:
      print(f"Annotation Content")
    elif result.kind == PdfQueryKind.annotation_extracted:
      print(f"Annotation Extracted Text")
    print(f"Distance: {result.distance}")

    files = service.files(result.pdf_hash)
    if len(files) > 0:
      print("Files:")
      for file in files:
        print(f"  {file}")

    print("----------------------------------------")
    print(result.text)
    print("")