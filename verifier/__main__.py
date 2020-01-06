#!/user/bin/env python3

import csv
import os
import shelve
import sqlite3
import sys
import yaml

from .classes import DirList
from .classes import Batch

class Config():
    def __init__(self, path):
        with open(path) as handle:
            for k, v in yaml.safe_load(handle).items():
                setattr(self, k.lower(), v)

def main():

    config = Config(sys.argv[1])
    invdir = os.path.join(config.rootdir, config.sourcedir)
    dbpath = os.path.join(config.rootdir, config.database)
    outdir = os.path.join(config.rootdir, config.outputdir)

    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()

    batches = {}

    for file in os.listdir(invdir):
        batchname, date, extra = file.split('_', 2)
        dirlist = DirList(os.path.join(invdir, file))
        if (batchname, date) in batches:
            batches[(batchname, date)].append(dirlist)
        else:
            batches[(batchname, date)] = [dirlist]

    summary_handle = open(os.path.join(outdir, 'summary.csv'), 'w', 1)
    changed_handle = open(os.path.join(outdir, 'changes.csv'), 'w', 1)

    for batchname, date in sorted(batches.keys()):
        print(f'\n{batchname.upper()}\n{"="*len(batchname)}')
        batch = Batch(batchname, date)
        print(f'Batch includes the following inventory files:')
        for n, dirlist in enumerate(batches[(batchname, date)], 1):
            print(f"  ({n}) {dirlist.filename}")
            batch.dirlists.append(dirlist)
            batch.load_from(dirlist, config.excludes)

        print(f"Total Assets: {len(batch.assets)}")
        lines = sum([len(dirlist.lines) for dirlist in batch.dirlists])
        print(f"Total Lines: {lines}")
        print(f"Checking database for copies of assets...")

        batch_summary = batch.lookup_assets(cursor)
        summary_handle.write(','.join(batch_summary) + '\n')

        for change in batch.changes:
            print(change)
            changed_handle.write(','.join(change) + '\n')

        batch.create_reports(outdir)

if __name__ == "__main__":
    main()
