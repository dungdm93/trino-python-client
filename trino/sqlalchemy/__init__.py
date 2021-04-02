from sqlalchemy.dialects import registry

__version__ = '0.2.0'
registry.register("trino", "trino.sqlalchemy.dialect.TrinoDialect", "TrinoDialect")
