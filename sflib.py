#!/usr/bin/env python3

from decimal import Decimal
import os
import unittest

import snowflake.connector


def connect(user, password, account, database):
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database
    )
    
def close(ctx):
    if ctx is not None:
        ctx.close()
        ctx = None
        return True
    return False

def bind_params(query, params):
    if params is None:
        return query
    
    for p in params:
        if isinstance(params[p], int) or isinstance(params[p], float):
            v = "{0}".format(params[p])
        else:
            v = "'{0}'".format(params[p])
        query = query.replace(':{0}:'.format(p), v)
    return query

def query(ctx, sql):
    cs = ctx.cursor()

    rs = []
    try:
        cs.execute(sql)
        cols = []
        for c in cs.description:
            cols.append(c[0].lower())
        for r in cs.fetchall():
            d = {}
            for a in zip(cols, r):
                d[a[0]] = a[1]
            rs.append(d)
    except snowflake.connector.errors.ProgrammingError as e:
        raise Exception('{0} ({1})'.format(e, sql))
    finally:
        cs.close()

    return rs


class TestSFlib(unittest.TestCase):
    def setUp(self):
        self.u = os.environ['snowflake_username']
        self.p = os.environ['snowflake_password']
        self.a = os.environ['snowflake_account']
        self.d = os.environ['snowflake_database']

    def _test_connect_001(self):
        ctx = connect(self.u, self.p, self.a, self.d)
        self.assertTrue(isinstance(ctx, snowflake.connector.connection.SnowflakeConnection))
        
    def _test_connect_002(self):
        with self.assertRaises(snowflake.connector.errors.DatabaseError):
            connect('nosuchuser', self.p, self.a, self.d)
        
    def _test_connect_003(self):
        ctx = connect(self.u, self.p, self.a, 'nosuchdatabase')
        self.assertTrue(isinstance(ctx, snowflake.connector.connection.SnowflakeConnection))
        
    def _test_close_001(self):
        ctx = connect(self.u, self.p, self.a, self.d)
        self.assertTrue(isinstance(ctx, snowflake.connector.connection.SnowflakeConnection))

        self.assertTrue(close(ctx))

    def test_query_001(self):
        ctx = connect(self.u, self.p, self.a, self.d)

        rs = query(ctx, 'select 1')
        self.assertEqual([{'1': 1}], rs)

        rs = query(ctx, 'SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER WHERE C_CUSTKEY = 60001')
        self.assertEqual([{'c_acctbal': Decimal('9957.56'),
                           'c_address': '9Ii4zQn9cX',
                           'c_comment': 'l theodolites boost slyly at the platelets: permanently ironic '
                           'packages wake slyly pend',
                           'c_custkey': 60001,
                           'c_mktsegment': 'HOUSEHOLD',
                           'c_name': 'Customer#000060001',
                           'c_nationkey': 14,
                           'c_phone': '24-678-784-9652'}], rs)

        self.assertTrue(close(ctx))

    def test_query_002(self):
        ctx = connect(self.u, self.p, self.a, self.d)

        rs = query(ctx, 'select 1 as p')
        self.assertEqual([{'p': 1}], rs)

        self.assertTrue(close(ctx))

    def test_bind_params_001(self):
        q = bind_params('select 1', None)
        self.assertEqual('select 1', q)
        
        q = bind_params('select 1', {'foo': 'bar'})
        self.assertEqual('select 1', q)
        
    def test_bind_params_002(self):
        q = bind_params('select :foo:', {'foo': 'bar'})
        self.assertEqual("select 'bar'", q)

        q = bind_params('select :foo:,:foo2:', {'foo': 'bar', 'foo2': 'bar2'})
        self.assertEqual("select 'bar','bar2'", q)

    def test_bind_params_003(self):
        q = bind_params("""
SELECT
  C_CUSTKEY
  ,C_NAME
  ,C_ADDRESS
  ,C_NATIONKEY
  ,C_PHONE
  ,C_ACCTBAL
  ,C_MKTSEGMENT
  ,C_COMMENT
FROM
  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
WHERE
  C_CUSTKEY = :custkey:
""", {'custkey': 1})

        self.assertEqual("""
SELECT
  C_CUSTKEY
  ,C_NAME
  ,C_ADDRESS
  ,C_NATIONKEY
  ,C_PHONE
  ,C_ACCTBAL
  ,C_MKTSEGMENT
  ,C_COMMENT
FROM
  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
WHERE
  C_CUSTKEY = 1
""", q)

    def _test_bind_and_query_001(self):
        ctx = connect(self.u, self.p, self.a, self.d)
        self.assertTrue(isinstance(ctx, snowflake.connector.connection.SnowflakeConnection))

        q = bind_params('select :foo:,:foo2:', {'foo': 'bar', 'foo2': 'bar2'})
        self.assertEqual("select 'bar','bar2'", q)

        rs = query(ctx, q)
        self.assertEqual([{"'bar'": 'bar', "'bar2'": 'bar2'}], rs)

        self.assertTrue(close(ctx))

    def test_bind_and_query_002(self):
        ctx = connect(self.u, self.p, self.a, self.d)
        self.assertTrue(isinstance(ctx, snowflake.connector.connection.SnowflakeConnection))

        q = bind_params('select :foo: as f1,:foo2: as f2', {'foo': 'bar', 'foo2': 'bar2'})
        self.assertEqual("select 'bar' as f1,'bar2' as f2", q)

        rs = query(ctx, q)
        self.assertEqual([{"f1": 'bar', "f2": 'bar2'}], rs)

        self.assertTrue(close(ctx))


if __name__ == '__main__':
    unittest.main()
