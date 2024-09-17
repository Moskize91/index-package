import os
import unittest

from index_package.segmentation import Segment
from index_package.index2.index_db import IndexDB
from index_package.index2 import FTS5DB, VectorDB, IndexNode, IndexNodeMatching
from tests.utils import get_temp_path

class TestPdfParser(unittest.TestCase):

  def test_fts5_query(self):
    db = FTS5DB(
      db_path=os.path.abspath(os.path.join(get_temp_path("index-database/fts5"), "db.sqlite3")),
    )
    db.save(
      node_id="id1",
      segments=[
        Segment(start=0, end=100, text="Transference interpretations, like extratransference interpretations or indeed any behavior on the analyst’s part."),
        Segment(start=100, end=250, text="the transference in the here and now are the core of the analytic work."),
      ],
      metadata={},
    )
    db.save(
      node_id="id2",
      segments=[
        Segment(start=0, end=100, text="I am of the opinion that the range of settings."),
        Segment(start=100, end=250, text="most  people  would  call  this  treatment \"psychotherapy.\""),
        Segment(start=250, end=350, text="which the  technique  of  analysis  of  the  transference is appropriate"),
      ],
      metadata={},
    )
    nodes: list[IndexNode] = []
    for node in db.query("Transference analysis"):
      nodes.append(node)

    self.assertEqual(len(nodes), 1)
    node = nodes[0]

    self.assertEqual(node.id, "id2")
    self.assertEqual(node.segments, [(0, 100), (100, 250), (250, 350)])

    nodes = []
    for node in db.query("Transference analysis", is_or_condition=True):
      nodes.append(node)

    self.assertEqual(len(nodes), 1)
    node = nodes[0]

    self.assertEqual(node.id, "id1")
    self.assertEqual(node.segments, [(0, 100), (100, 250)])

    db.remove("id2")
    nodes = []
    for node in db.query("Transference analysis"):
      nodes.append(node)

    self.assertEqual(len(nodes), 0)

  def test_vector_query(self):
    db = VectorDB(
      index_dir_path=get_temp_path("index-database/vector"),
      embedding_model_id="shibing624/text2vec-base-chinese"
    )
    db.save(
      node_id="index/db/id1",
      segments=[
        Segment(start=0, end=100, text="Transference interpretations, like extratransference interpretations or indeed any behavior on the analyst’s part."),
        Segment(start=100, end=250, text="the transference in the here and now are the core of the analytic work."),
      ],
      metadata={},
    )
    db.save(
      node_id="index/db/id2",
      segments=[
        Segment(start=0, end=100, text="I am of the opinion that the range of settings."),
        Segment(start=100, end=250, text="most  people  would  call  this  treatment \"psychotherapy.\""),
        Segment(start=250, end=350, text="which the  technique  of  analysis  of  the  transference is appropriate"),
      ],
      metadata={},
    )
    nodes = db.query("the transference in the here and now are the core of the analytic work.", results_limit=1)
    self.assertEqual(len(nodes), 1)
    node = nodes[0]
    self.assertEqual(node.id, "index/db/id1")

  def test_database_query(self):
    fts5_db = FTS5DB(
      db_path=os.path.abspath(os.path.join(get_temp_path("index-database/database"), "db.sqlite3")),
    )
    vector_db = VectorDB(
      index_dir_path=get_temp_path("index-database/database/vector"),
      embedding_model_id="shibing624/text2vec-base-chinese"
    )
    db = IndexDB(
      fts5_db=fts5_db,
      vector_db= vector_db,
    )
    db.save(
      node_id="id1",
      segments=[Segment(start=0, end=200, text="which the technique  of  analysis  of  the  transference is appropriate")],
      metadata={},
    )
    db.save(
      node_id="id2",
      segments=[Segment(start=0, end=200, text="most  people  would  call  this  treatment \"psychotherapy.\"")],
      metadata={},
    )
    db.save(
      node_id="id3",
      segments=[Segment(start=0, end=200, text="the transference in the here and now are the core of the analytic work.")],
      metadata={},
    )
    db.save(
      node_id="id4",
      segments=[Segment(start=0, end=200, text="Transference interpretations, like extratransference interpretations or indeed any behavior on the analyst’s part.")],
      metadata={},
    )
    db.save(
      node_id="id5",
      segments=[Segment(start=0, end=200, text="the transference in the here and now are the core of the analytic work.")],
      metadata={},
    )
    results: list[tuple[str, IndexNodeMatching]] = []

    for node in db.query("Transference analysis", results_limit=100):
      results.append((node.id, node.matching))

    self.assertEqual(results, [
      ("id1", IndexNodeMatching.Matched),
      ("id3", IndexNodeMatching.MatchedPartial),
      ("id4", IndexNodeMatching.MatchedPartial),
      ("id5", IndexNodeMatching.MatchedPartial),
      ("id2", IndexNodeMatching.Similarity),
    ])