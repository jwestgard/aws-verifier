#!/usr/bin/env python3

import sqlite3
import sys


class File():
    """CREATE TABLE files(
        uuid       TEXT PRIMARY KEY UNIQUE NOT NULL,
        bytes      INTEGER,
        md5        TEXT,
        filename   TEXT,
        path       TEXT,
        sourcefile INTEGER,
        sourceline INTEGER,
        action     TEXT,
        FOREIGN KEY(sourcefile) REFERENCES dirlists(id)
    );"""

    def __init__(self):
        pass


class Dirlist():
    """CREATE TABLE dirlists(
        id         INTEGER PRIMARY KEY UNIQUE NOT NULL,
        md5        TEXT,
        filename   TEXT,
        share      TEXT,
        batch      TEXT
    );"""

    def __init__(self):
        pass


class Accession():
    """CREATE TABLE accessions(
        id         INTEGER PRIMARY KEY UNIQUE NOT NULL,
        filename   TEXT,
        bytes      INTEGER,
        md5        TEXT,
        sourcefile INTEGER,
        sourceline INTEGER,
        FOREIGN KEY(sourceFile) REFERENCES dirlists(id)
    );"""

    def __init__(self):
        pass


class AccessionBatch():
    """CREATE TABLE accession_batches(
        id         INTEGER PRIMARY KEY UNIQUE NOT NULL,
        name       TEXT,
        date       TEXT
    );"""

    def __init__(self):
        pass


class Database():
    pass


def save_accession(asset, cursor):
    data = (asset.filename, 
            asset.bytes, 
            asset.filename, 
            asset.sourcefile, 
            asset.sourceline
            )
    query = '''INSERT INTO accessions (md5, filename, bytes, sourcefile, sourceline)
                VALUES (?, ?, ?, ?, ?)'''
    result = cursor.execute(query, data)
    if result:
        return result.lastrowid
    else:
        return None


def main():

    # (1) load configuration
    configfile = sys.argv[1]
    with open(configfile) as handle:
        config = yaml.safe_load(handle)
    package_root = os.path.join(config['ROOTDIR'], config['OUTPUTDIR'])
    source_root = os.path.join(config['ROOTDIR'], config['SOURCEDIR'])
    database = Database(os.path.join(config['ROOTDIR'], config['DATABASE']))
    package = Package(package_root)
    exclude_patterns = config['EXCLUDES']

    # (2) set up the set of all accession batches
    batches = {}
    for file in os.listdir(source_root):
        try:
            batchname, date, extra = file.split('_', 2)
            dirlist = accessions.DirList(os.path.join(source_root, file))
            batches.setdefault(batchname, []).append(dirlist)
        except ValueError:
            sys.stdout.write(f"Could not parse file: {file}\n")
            sys.exit(1)

    # (3) Read accessions
    for batchname in sorted(batches.keys()):
        print(f'\n{batchname.upper()}\n{"=" * len(batchname)}')
        batch = accessions.Batch(batchname, *batches[batchname])
        print(f"  Creating {batch.identifier}...")
        print(f"  Source Files: {len(batch.dirlists)}")
        for n, dirlist in enumerate(batch.dirlists, 1):
            print(f"    ({n}) {dirlist.filename}: {len(dirlist.lines)} lines")
        print(f"  Total Assets: {len(batch.assets)}")

        # (4) Process assets
        print(f"  Processing batch assets...")
        for n, asset in enumerate(batch.assets, 1):
            save_accession(asset, database)


if __name__ == "__main__":
    main()