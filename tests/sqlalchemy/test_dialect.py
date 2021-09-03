from typing import List, Any, Dict
from unittest import mock

import pytest
from assertpy import assert_that
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL

from trino.auth import BasicAuthentication
from trino.dbapi import Connection
from trino.sqlalchemy.dialect import TrinoDialect
from trino.transaction import IsolationLevel


class TestTrinoDialect:
    def setup(self):
        self.dialect = TrinoDialect()

    @pytest.mark.parametrize(
        'url, expected_args, expected_kwargs',
        [
            (make_url('trino://localhost'),
             list(), dict(host='localhost', catalog='system', user='anonymous')),
            (make_url('trino://1.2.3.4:4321/mysql/sakila'),
             list(), dict(host='1.2.3.4', port=4321, catalog='mysql', schema='sakila', user='anonymous')),
            (make_url('trino://user@localhost:8080'),
             list(), dict(host='localhost', port=8080, catalog='system', user='user')),
            (make_url('trino://user:pass@localhost:8080'),
             list(), dict(host='localhost', port=8080, catalog='system', user='user',
                          auth=BasicAuthentication('user', 'pass'), http_scheme='https')),
        ],
    )
    def test_create_connect_args(self, url: URL, expected_args: List[Any], expected_kwargs: Dict[str, Any]):
        actual_args, actual_kwargs = self.dialect.create_connect_args(url)

        assert_that(actual_args).is_equal_to(expected_args)
        assert_that(actual_kwargs).is_equal_to(expected_kwargs)

    def test_get_default_isolation_level(self):
        isolation_level = self.dialect.get_default_isolation_level(mock.Mock())
        assert_that(isolation_level).is_equal_to('AUTOCOMMIT')

    def test_isolation_level(self):
        dbapi_conn = Connection(host="localhost")

        self.dialect.set_isolation_level(dbapi_conn, "SERIALIZABLE")
        assert_that(dbapi_conn._isolation_level).is_equal_to(IsolationLevel.SERIALIZABLE)

        isolation_level = self.dialect.get_isolation_level(dbapi_conn)
        assert_that(isolation_level).is_equal_to("SERIALIZABLE")
