from dtest import Tester
from tools import since, rows_to_list
from assertions import assert_invalid
from cassandra.protocol import ConfigurationException
import time


class TestSchema(Tester):

    @since('2.0')
    def drop_column_compact_test(self):
        cursor = self.prepare()

        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        assert_invalid(cursor, "ALTER TABLE cf DROP c1", "Cannot drop columns from a")

    @since('2.0')
    def drop_column_compaction_test(self):
        cursor = self.prepare()
        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

        # insert some data.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        cursor.execute("ALTER TABLE cf DROP c1")
        cursor.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        node = self.cluster.nodelist()[0]
        node.flush()
        node.compact()

        # erase info on dropped 'c1' column and restart.
        cursor.execute("""UPDATE system.schema_columnfamilies
                          SET dropped_columns = null
                          WHERE keyspace_name = 'ks' AND columnfamily_name = 'cf'""")
        node.stop(gently=False)
        node.start()
        time.sleep(.5)

        # test that c1 values have been compacted away.
        cursor = self.patient_cql_connection(node, version='3.0.10')
        rows = cursor.execute("SELECT c1 FROM ks.cf")
        self.assertEqual([[None], [None], [None], [4]], sorted(rows_to_list(rows)))

    @since('2.0')
    def drop_column_queries_test(self):
        cursor = self.prepare()

        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")
        cursor.execute("CREATE INDEX ON cf(c2)")

        # insert some data.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        cursor.execute("ALTER TABLE cf DROP c1")
        cursor.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        rows = cursor.execute("SELECT c1 FROM cf")
        self.assertEqual([[None], [None], [None], [4]], sorted(rows_to_list(rows)))

        rows = cursor.execute("SELECT * FROM cf")
        self.assertEqual([[0,None,2], [1,None,3], [2,None,4], [3,4,5]], sorted(rows_to_list(rows)))

        rows = cursor.execute("SELECT c1 FROM cf WHERE key = 0")
        self.assertEqual([[None]], rows_to_list(rows))

        rows = cursor.execute("SELECT c1 FROM cf WHERE key = 3")
        self.assertEqual([[4]], rows_to_list(rows))

        rows = cursor.execute("SELECT * FROM cf WHERE c2 = 2")
        self.assertEqual([[0,None,2]], rows_to_list(rows))

        rows = cursor.execute("SELECT * FROM cf WHERE c2 = 5")
        self.assertEqual([[3,4,5]], rows_to_list(rows))

    def alter_clustering_column_test(self):
        """ Test for CASSANDRA-8879 """
        cursor = self.prepare()

        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf1 (a int, b blob, c int, PRIMARY KEY (a, b))")

        # this should fail, because we can't go from blob to ascii
        assert_invalid(cursor, "ALTER TABLE cf1 ALTER b TYPE ascii", expected=ConfigurationException)

        # but we can go from ascii to blob
        cursor.execute("CREATE TABLE cf2 (a int, b ascii, c int, PRIMARY KEY (a, b))")
        cursor.execute("ALTER TABLE cf2 ALTER b TYPE blob")

    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        cursor = self.patient_cql_connection(nodes[0], version='3.0.10')
        self.create_ks(cursor, 'ks', 1)
        return cursor
