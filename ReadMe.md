csv-to-sqlite
=============

This is a simple, **datatype-guessing** script that takes CSV files as input and copies their contents into a SQLite database. .
(Column names are taken from the headers (first row) in the csv file.) 
Intended for **Python 3**. (I know it *did* run on Py2 as some people tried, but I haven't tested it.)

## Links 

* [Source on GitHub](https://github.com/zblesk/csv-to-sqlite) 
* [PyPI page](https://pypi.org/project/csv-to-sqlite/) 
* [Introductory blog post with basic intro + a how-to](http://zblesk.net/blog/csv-to-sqlite/)
* [Other related blog posts](https://zblesk.net/blog/tag/csv-to-sqlite/) 

## Overview

Installs via 

```
 pip install csv-to-sqlite
```

To find out more, run

```
 csv-to-sqlite --help
```

If you've installed the package as a dependency for your own script, you can use it like this:

```python
import csv_to_sqlite 

# all the usual options are supported
options = csv_to_sqlite.CsvOptions(typing_style="full", encoding="windows-1250") 
input_files = ["abilities.csv", "moves.csv"] # pass in a list of CSV files
csv_to_sqlite.write_csv(input_files, "output.sqlite", options)
```

