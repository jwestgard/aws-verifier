#!/usr/bin/env python3

import os
import sqlite3
import sys
import yaml

from accessions import DirList
from accessions import Batch
from accessions import Asset

from restores import Database


def main():
    configfile = sys.argv[1]
    with open(configfile) as handle:
        config = yaml.safe_load(handle)
    source_root = os.path.join(config['ROOTDIR'], config['SOURCEDIR'])
    database = Database(os.path.join(config['ROOTDIR'], config['DATABASE']))
    exclude_patterns = config['EXCLUDES']

    batches = {}
    for file in os.listdir(source_root):
        try:
            batchname, date, extra = file.split('_', 2)
            dirlist = DirList(os.path.join(source_root, file))
            batches.setdefault(batchname, []).append(dirlist)
        except ValueError:
            sys.stdout.write(f"Could not parse file: {file}\n")
            sys.exit(1)

    for batchname in sorted(batches.keys()):
        print(f'\n{batchname.upper()}\n{"=" * len(batchname)}')
        batch = Batch(batchname, *batches[batchname])
        id = database.lookup_batch(batch)
        print(id)

        '''
        print(f"  Creating {batch.identifier}...")
        print(f"  Source Files: {len(batch.dirlists)}")
        for n, dirlist in enumerate(batch.dirlists, 1):
            print(f"    ({n}) {dirlist.filename}: {len(dirlist.lines)} lines")
        print(f"  Total Assets: {len(batch.assets)}")

        print(f"  Processing batch assets...")
        #for n, asset in enumerate(batch.assets, 1):
        #    save_accession(asset, database)
        '''

if __name__ == "__main__":
    main()
