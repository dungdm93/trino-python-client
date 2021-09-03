from typing import List, Any, Dict

import pytest
from assertpy import assert_that
from sqlalchemy.engine import make_url
from sqlalchemy.engine.url import URL

from trino.auth import BasicAuthentication
from trino.sqlalchemy.dialect import TrinoDialect


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
