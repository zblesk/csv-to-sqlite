import pytest
import csv_to_sqlite 
from os import path

def test_csv_script():
    options = csv_to_sqlite.CsvOptions(typing_style="quick", drop_tables=True) 
    input_files = ["tests\\data\\abilities.csv"] 
    total = csv_to_sqlite.write_csv(input_files, "test_out.sqlite", options)
    assert total == 293
    assert path.exists("test_out.sqlite")