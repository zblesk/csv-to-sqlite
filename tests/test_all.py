import pytest
import csv_to_sqlite 
import sqlite3
from os import path

def test_csv_script():
    options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True) 
    input_files = ["tests/data/abilities.csv"] 
    total = csv_to_sqlite.write_csv(input_files, "test_out.sqlite", options)
    assert total == 293
    assert path.exists("test_out.sqlite")

def test_csv_basic():
    options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True) 
    input_files = ["tests/data/abilities.csv", "tests/data/moves.csv", "tests/data/natures.csv"] 
    total = csv_to_sqlite.write_csv(input_files, "multiple.sqlite", options)
    assert total == 1064
    assert path.exists("multiple.sqlite")
    connection = sqlite3.connect("multiple.sqlite")
    tables = connection.execute("SELECT * FROM sqlite_master;").fetchall()
    assert len(tables) == 3
    movesSql = connection.execute("SELECT sql FROM sqlite_master where name = 'natures';").fetchall()
    assert len(movesSql) == 1
    assert movesSql[0][0] == "CREATE TABLE [natures] (\n\t[id] integer,\n\t[identifier] text,\n\t[decreased_stat_id] integer,\n\t[increased_stat_id] integer,\n\t[hates_flavor_id] integer,\n\t[likes_flavor_id] integer,\n\t[game_index] integer\n)"



