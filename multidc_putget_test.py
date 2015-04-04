from dtest import Tester
from tools import putget
from ccmlib.cluster import Cluster
from assertions import assert_one, assert_none
from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement

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
        cluster.populate([3, 2]).set_configuration_options({'in_memory_compaction_limit_in_mb' : '1'}).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', {'dc1' : 2, 'dc1': 1})
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
        for i in xrange(4000000):
            session.execute_async(insert_query, ['a', '%d' % i, 'Here is my message, it helps to make things longer to hit limits for files.', i])
            if i % 100000 == 0:
                node1.flush()

        for i in xrange(10000):
            assert_one(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, ['a', '%s' % i, 'Here is my message, it helps to make things longer to hit limits for files.', i], cl=CL.ALL)
        for i in xrange(10000):
            session.execute(SimpleStatement("DELETE FROM bounces WHERE domainid = 'a' and address = '%s'" % i, consistency_level=CL.ALL))

        node1.flush()
        node1.compact()

        for i in xrange(10000):
            assert_none(session, "SELECT * FROM bounces WHERE domainid = 'a' and address='%s'" % i, cl=CL.ALL)
