import os
import unittest

from index_package.segmentation import Segment
from index_package.index2 import FTS5DB, VectorDB, IndexNode
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
    nodes = db.query("the transference in the here and now are the core of the analytic work.", results_limit=1)
    self.assertEqual(len(nodes), 1)
    node = nodes[0]
    self.assertEqual(node.id, "id1")