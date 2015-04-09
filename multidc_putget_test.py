import random
from dtest import Tester, debug
from tools import putget
from ccmlib.cluster import Cluster
from assertions import assert_one, assert_none
from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

class TestMultiDCPutGet(Tester):

    def putget_2dc_rf1_test(self):
        """ Simple put-get test for 2 DC with one node each (RF=1) [catches #3539] """
        cluster = self.cluster
        cluster.populate([1, 1]).start()

        cursor = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(cursor, 'ks', { 'dc1' : 1, 'dc2' : 1})
        self.create_cf(cursor, 'cf')

        putget(cluster, cursor)

    def putget_2dc_rf2_test(self):
        """ Simple put-get test for 2 DC with 2 node each (RF=2) -- tests cross-DC efficient writes """
        cluster = self.cluster
        cluster.populate([2, 2]).start()

        cursor = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(cursor, 'ks', { 'dc1' : 2, 'dc2' : 2})
        self.create_cf(cursor, 'cf')

        putget(cluster, cursor)

    def test_9045(self):
        cluster = self.cluster
        cluster.populate(3).set_configuration_options({'in_memory_compaction_limit_in_mb': '1', 'write_request_timeout_in_ms': '10000'}).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 3)
        session.execute("""CREATE TABLE bounces (
            domainid text,
            address text,
            message text,
            t bigint,
            PRIMARY KEY (domainid, address)
            ) WITH
            bloom_filter_fp_chance=0.100000 AND
            caching='KEYS_ONLY' AND
            comment='' AND
            dclocal_read_repair_chance=0.100000 AND
            gc_grace_seconds=864000 AND
            index_interval=128 AND
            read_repair_chance=0.000000 AND
            populate_io_cache_on_flush='false' AND
            default_time_to_live=0 AND
            speculative_retry='99.0PERCENTILE' AND
            memtable_flush_period_in_ms=0 AND
            compaction={'class': 'LeveledCompactionStrategy'} AND
            compression={'sstable_compression': 'LZ4Compressor'};""")

        insert_query = session.prepare("INSERT INTO bounces (domainid, address, message, t) VALUES (?, ?, ?, ?)")
        insert_query.consistency_level = CL.ALL
        node1 = self.cluster.nodelist()[0]
        message = "a" * 1000
        outer_rounds = 400
        inner_rounds = 10000
        for outer in range(outer_rounds):
            debug("Inserting round %d" % outer)
            args = [('a', str(outer * inner_rounds + i), message, outer * inner_rounds + i) for i in xrange(inner_rounds)]
            execute_concurrent_with_args(session, insert_query, args)
            node1.flush()

        num_rows = outer_rounds * inner_rounds
        to_delete = set([random.randint(0, num_rows - 1) for i in xrange(10000)])
        print "rows to delete:", to_delete

        debug("Selecing rows")
        for i in to_delete:
            assert_one(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, ['a', '%s' % i, message, i], cl=CL.ALL)

        debug("Updating rows")
        for i in to_delete:
            session.execute(SimpleStatement("UPDATE bounces SET message = '%s' WHERE domainid = 'a' AND address = '%s'" % (message, i), consistency_level=CL.ALL))

        node1.flush()

        debug("Deleting rows")
        for i in to_delete:
            session.execute(SimpleStatement("DELETE FROM bounces WHERE domainid = 'a' and address = '%s'" % i, consistency_level=CL.ALL))

        node1.flush()
        node1.compact()

        debug("Selecting rows")
        for i in to_delete:
            assert_none(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, cl=CL.ONE)

        debug("Repairing node1")
        node1.repair(["-pr", "ks", "bounces"])

        debug("Selecting rows")
        for i in to_delete:
            assert_none(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, cl=CL.ONE)

        debug("Repairing node2")
        node2 = self.cluster.nodelist()[1]
        node2.repair(["-pr", "ks", "bounces"])

        debug("Selecting rows")
        for i in to_delete:
            assert_none(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, cl=CL.ONE)

        debug("Repairing node2")
        node3 = self.cluster.nodelist()[2]
        node3.repair(["-pr", "ks", "bounces"])

        debug("Selecting rows")
        for i in to_delete:
            assert_none(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, cl=CL.ONE)
