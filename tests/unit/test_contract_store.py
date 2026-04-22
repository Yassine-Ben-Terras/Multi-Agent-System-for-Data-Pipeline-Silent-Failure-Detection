"""
Unit tests for SchemaContract hash computation and ColumnDef helpers.
ContractStore DB methods are tested via mocking — SQL correctness
is verified in integration tests with a real Postgres instance.
"""

from __future__ import annotations

import pytest
from agents.schema_watcher.contract_store import ColumnDef, SchemaContract


class TestSchemaContractHash:

    def test_same_columns_same_hash(self):
        cols = [ColumnDef("id", "VARCHAR", False), ColumnDef("amt", "FLOAT", True)]
        h1 = SchemaContract.compute_hash(cols)
        h2 = SchemaContract.compute_hash(cols)
        assert h1 == h2

    def test_different_columns_different_hash(self):
        cols_a = [ColumnDef("id", "VARCHAR", False)]
        cols_b = [ColumnDef("id", "INTEGER", False)]
        assert SchemaContract.compute_hash(cols_a) != SchemaContract.compute_hash(cols_b)

    def test_hash_is_16_chars(self):
        cols = [ColumnDef("x", "INT", True)]
        assert len(SchemaContract.compute_hash(cols)) == 16

    def test_as_dict_indexed_by_name(self):
        cols = [ColumnDef("id", "VARCHAR", False), ColumnDef("name", "TEXT", True)]
        contract = SchemaContract("src", cols, "abc")
        d = contract.as_dict()
        assert "id" in d
        assert "name" in d
        assert d["id"].dtype == "VARCHAR"
