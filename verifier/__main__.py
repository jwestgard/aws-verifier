#!/user/bin/env python3

import csv
import os
import shelve
import sqlite3
import sys
import yaml

from .accessions import DirList
from .database import Database
from .uploads import Package


class Batch():
    """
    Class representing one or more groups of assets being preserved.
    """

    def __init__(self, configfile):
        with open(configfile) as handle:
            config = yaml.safe_load(handle)

        self.rootdir = config['ROOTDIR']
        self.sourcedir = os.path.join(self.rootdir, config['SOURCEDIR'])
        self.outputdir = os.path.join(self.rootdir, config['OUTPUTDIR'])
        self.exclude_patterns = config['EXCLUDES']

        # set up database connection
        db_path = os.path.join(self.rootdir, config['DATABASE'])
        self.db = Database(db_path)

        self.sets = {}
        for file in os.listdir(self.sourcedir):
            try:
                batchname, date, extra = file.split('_', 2)
                dirlist = DirList(os.path.join(self.sourcedir, file))
                self.sets.setdefault(batchname, []).append(dirlist)
            except ValueError:
                sys.stdout.write(f"Could not parse filename {file}\n")

        #print(self.batches)




    def foo(self, name, date):
        self.name = name
        self.date = date
        self.assets = []
        self.dirlists = []
        self.accessions = []
        self.excluded = []
        self.missing = []
        self.changes = []

    def load_from(self, source, exclude_patterns):
        for asset in source.assets():
            if asset.filename in exclude_patterns:
                self.excluded.append(asset)
            else:
                self.assets.append(asset)

    def lookup_assets(self, cursor):
        total_assets = len(self.assets)
        matches = 0
        duplicates = 0
        not_found = 0
        for asset in self.assets:
            count, changes = asset.check_status(cursor)
            if changes:
                self.changes.extend(changes)
            if count == 0:
                print(f"{asset.filename} not found!")
                not_found += 1
            elif count == 1:
                matches += 1
            else:
                matches += 1
                duplicates += (count - 1)
        print((f"Total: {total_assets}, ",
               f"Matches: {matches}, ",
               f"Duplicates: {duplicates}, ",
               f"Not Found: {not_found}"))
        return (self.name, self.date, str(total_assets),
                str(matches), str(duplicates), str(not_found))


    def create_reports(self, root):
        fieldnames = ['md5', 'filename', 'bytes', 'timestamp']
        batchdir = os.path.join(root, self.name)
        if not os.path.exists(batchdir):
            os.makedirs(batchdir)
        # Write the various categories of asset to their respective files
        for data, file in [
            (self.assets, 'accessions.csv'),
            (self.excluded, 'excludes.csv'),
            (self.missing, 'missing.csv')
            ]:
            path = os.path.join(batchdir, file)
            print(f'Writing to {path}...')
            with open(path, 'w') as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                for asset in data:
                    writer.writerow({'md5': asset.md5,
                                     'filename': asset.filename,
                                     'bytes': asset.bytes,
                                     'timestamp': asset.timestamp
                                     })



def main():

    config = sys.argv[1]
    batch = Batch(config)
    print(len(batch.sets))


    """
    invdir = os.path.join(config.rootdir, config.sourcedir)
    dbpath = os.path.join(config.rootdir, config.database)
    outdir = os.path.join(config.rootdir, config.outputdir)

    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()

    batches = {}

    for file in os.listdir(invdir):
        try:
            batchname, date, extra = file.split('_', 2)
            dirlist = DirList(os.path.join(invdir, file))
            if (batchname, date) in batches:
                batches[(batchname, date)].append(dirlist)
            else:
                batches[(batchname, date)] = [dirlist]
        except ValueError:
            sys.stdout.write(f"Could not parse filename {file}\n")

    '''
    summary_handle = open(os.path.join(outdir, 'summary.csv'), 'w', 1)
    changed_handle = open(os.path.join(outdir, 'changes.csv'), 'w', 1)
    '''

    package = Package(outdir)

    for batchname, date in sorted(batches.keys()):
        print(f'\n{batchname.upper()}\n{"="*len(batchname)}')
        batch = Batch(batchname, date)
        print(f' Batch includes the following inventory files:')
        for n, dirlist in enumerate(batches[(batchname, date)], 1):
            print(f"  ({n}) {dirlist.filename} ({len(dirlist.assets())})")
            batch.dirlists.append(dirlist)
            batch.load_from(dirlist, config.excludes)
            batch.lookup_assets(cursor)

        package.add_batch_folder(batch.name)

        '''
        print(f" Accessions: {len(batch.accessions)}")
        print(f" Excludes:   {len(batch.excluded)}")
        print(f" Missing:    {len(batch.missing)}")
        print(f" Changed:    {len(batch.changes)}")
        '''

        '''
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
        '''
        """

if __name__ == "__main__":
    main()
