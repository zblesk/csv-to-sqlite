"""A script that processes the input CSV files and copies them into a SQLite database."""
import csv
import sqlite3
import os
import sys
import click
import time


__version__ = '1.2.0'

def write_out(msg):
    if write_out.verbose:
        print(msg)


class CsvOptions:
    def __init__(self, determine_column_types=True, 
                 drop_tables=False, delimiter=","):
        self.determine_column_types = determine_column_types
        self.drop_tables = drop_tables
        self.delimiter = delimiter

class CsvFileInfo:
    def __init__(self, path, options = None):
        self.path = path
        self.columnNames = None
        self.columnTypes = None
        self.csvfile = None
        self.reader = None
        self.options = options
        if not options:
            self.options = CsvOptions()

    def get_table_name(self):
        return os.path.splitext(os.path.basename(self.path))[0]

    def get_minimal_type(self, value):
        try:
            int(value)
            return "integer"
        except ValueError:
            pass
        try:
            float(value)
            return "real"
        except ValueError:
            pass
        return "text"

    def __enter__(self):
        self.csvfile = open(self.path, encoding="utf8") 
        self.reader = csv.reader(self.csvfile, delimiter=self.options.delimiter)
        return self

    def __exit__(self, *args):
        if self.csvfile:
            self.csvfile.close()

    def get_restarted_reader(self):
        self.csvfile.seek(0)
        return self.reader

    def determine_types(self):
        write_out("Determining types")
        rdr = self.get_restarted_reader()
        self.columnNames = [name for name in next(rdr)]
        cols = len(self.columnNames)
        self.columnTypes = ["string"] * cols if not self.options.determine_column_types  else ["integer"] * cols
        for row in rdr:
            for col in range(cols):
                if self.columnTypes[col] == "text":
                    continue
                col_type = self.get_minimal_type(row[col])
                if self.columnTypes[col] != col_type:
                    if col_type == "text" or \
                            (col_type == "real" and self.columnTypes[col] == "integer"):
                        self.columnTypes[col] = col_type

    def save_to_db(self, connection):
        write_out("Writing table " + self.get_table_name())
        cols = len(self.columnNames)
        if self.options.drop_tables:
            try:
                write_out("Dropping table " + self.get_table_name())
                connection.execute('drop table [{tableName}]'.format(tableName=self.get_table_name()))
            except:
                pass
        connection.execute('create table [{tableName}] (\n'.format(tableName=self.get_table_name()) +
                           ',\n'.join("\t[%s] %s" % (i[0], i[1]) for i in zip(self.columnNames, self.columnTypes)) +
                           '\n);')
        linesTotal = 0
        currentBatch = 0
        reader = self.get_restarted_reader()
        buf = []
        maxL = 1000
        next(reader) #skip headers
        for line in reader:
            buf.append(line)
            currentBatch += 1
            if currentBatch == maxL:
                write_out("Inserting {0} records into {1}".format(maxL, self.get_table_name()))
                connection.executemany('insert into [{tableName}] values ({cols})'
                                    .format(tableName=self.get_table_name(), cols=','.join(['?'] * cols)),
                                    buf)
                linesTotal += currentBatch
                currentBatch = 0
                buf = []
        return linesTotal


@click.command()
@click.option("--file", "-f",
              type=click.Path(exists=True),
              help="A file to copy into the database. \nCan be specified multiple times. \n"
                   "All the files are processed, including file names piped from standard input.",
              multiple=True)
@click.option("--output", "-o", help="The output database path",
              type=click.Path(),
              default=os.path.basename(os.getcwd()) + ".db")
@click.option("--find-types/--no-types",
              help="Determines whether the script should guess the column type (int/float/string supported)",
              default=True)
@click.option("--drop-tables/--no-drop-tables", "-D",
              help="Determines whether the tables should be dropped before creation, if they already exist"
                   "(BEWARE OF DATA LOSS)",
              default=False)
@click.option("--verbose", "-v",
              is_flag=True,
              help="Determines whether progress reporting messages should be printed",
              default=False)
@click.option("--delimiter", "-x",
              help="Choose the CSV delimiter. Defaults to comma. Hint: for tabs, in Bash use $'\\t'.",
              default=",")
def start(file, output, find_types, drop_tables, verbose, delimiter):
    """A script that processes the input CSV files and copies them into a SQLite database.
    Each file is copied into a separate table. Column names are taken from the headers (first row) in the csv file.

    If file names are passed both via the --file option and standard input, all of them are processed.

    For example in PowerShell, if you want to copy all the csv files in the current folder
    into a database called "out.db", type:

        ls *.csv | % FullName | csv-to-sqlite -o out.db
    """
    write_out.verbose = verbose
    files = list(file)
    if not sys.stdin.isatty():
        files.extend(list(sys.stdin))
    if not files:
        write_out("No files were specified. Exiting.")
        return
    write_out("Output file: " + output)
    conn = sqlite3.connect(output)
    defaults = CsvOptions(determine_column_types=find_types, drop_tables=drop_tables, delimiter=delimiter)
    totalRowsInserted = 0
    startTime = time.perf_counter()
    with click.progressbar(files) as _files:
        actual = files if verbose else _files
        for file in actual:
            try:
                file = file.strip()
                write_out("Processing " + file)
                with CsvFileInfo(file, defaults) as info:
                    info.determine_types()
                    totalRowsInserted += info.save_to_db(conn)
            except Exception as exc:
                print("Error on table {0}: \n {1}".format(info.get_table_name(), exc))
    print("Written {0} rows into {1} tables in {2:.3f} seconds".format(totalRowsInserted, len(files), time.perf_counter() - startTime))
    conn.commit()

if __name__ == "__main__":
    start()
