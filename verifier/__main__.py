#!/user/bin/env python3

import os
import sys
import yaml

from . import accessions
from .restores import Database
from .archiver import Package


def main():

    # (1) load configuration
    configfile = sys.argv[1]
    with open(configfile) as handle:
        config = yaml.safe_load(handle)

    # (2) set up the set of all accession batches
    batches = {}
    root = os.path.join(config['ROOTDIR'], config['SOURCEDIR'])
    for file in os.listdir(root):
            try:
                batchname, date, extra = file.split('_', 2)
                dirlist = accessions.DirList(os.path.join(root, file))
                batches.setdefault(batchname, []).append(dirlist)
            except ValueError:
                sys.stdout.write(f"Could not parse filename {file}\n")
    
    #print(batches)
    
    # (3) Read accessions
    for batchname in sorted(batches.keys()):
        dirlists = batches[batchname]
        batch = accessions.Batch(batchname, *dirlists)
        print(batch.identifier, batch.has_hashes())
        
    #   a. check for md5
    #   b. exclude invisibles
    #   c. validate counts
    #   d. deduplicate
    
    # (4) Lookup restored files
    
    # (5) Write out archiver package


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
