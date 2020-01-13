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
    source_root = os.path.join(config['ROOTDIR'], config['SOURCEDIR'])
    output_root = os.path.join(config['ROOTDIR'], config['OUTPUTDIR'])
    db = Database(os.path.join(config['ROOTDIR'], config['DATABASE']))

    # (2) set up the set of all accession batches
    batches = {}
    for file in os.listdir(source_root):
        try:
            batchname, date, extra = file.split('_', 2)
            dirlist = accessions.DirList(os.path.join(source_root, file))
            batches.setdefault(batchname, []).append(dirlist)
        except ValueError:
            sys.stdout.write(f"Could not parse filename {file}\n")
            sys.exit(1)

    # (3) Read accessions
    for batchname in sorted(batches.keys()):
        print(f'\n{batchname.upper()}\n{"=" * len(batchname)}')
        batch = accessions.Batch(batchname, *batches[batchname]) 
        print(f"Creating {batch.identifier}...")
        print(f"Source Files: {len(batch.dirlists)}")
        for n, dirlist in enumerate(batch.dirlists, 1):
            print(f"  ({n}) {dirlist.filename}: {len(dirlist.lines)} lines")
        print(f"Total Assets: {len(batch.assets)}")
        
        #   a. check for md5
        #   b. exclude invisibles
        #   c. validate counts
        #   d. deduplicate
    
        # (4) Lookup restored files
        print(f"Querying database for copies of assets...")
        if batch.identifier == "Archive130":
            for asset in batch.assets[:1]:
                matches = db.best_matches_for(asset)
                print(matches)

    # (5) Write out archiver package
    package = Package(output_root)

    """
    summary_handle = open(os.path.join(outdir, 'summary.csv'), 'w', 1)
    changed_handle = open(os.path.join(outdir, 'changes.csv'), 'w', 1)

        print(f' Batch includes the following inventory files:')
            print(f"  ({n}) {dirlist.filename} ({len(dirlist.assets())})")
            batch.dirlists.append(dirlist)
            batch.load_from(dirlist, config.excludes)
            batch.lookup_assets(cursor)
        package.add_batch_folder(batch.name)

        print(f" Accessions: {len(batch.accessions)}")
        print(f" Excludes:   {len(batch.excluded)}")
        print(f" Missing:    {len(batch.missing)}")
        print(f" Changed:    {len(batch.changes)}")

        batch_summary = batch.lookup_assets(cursor)
        summary_handle.write(','.join(batch_summary) + '\n')

        for change in batch.changes:
            print(change)
            changed_handle.write(','.join(change) + '\n')

        batch.create_reports(outdir)
    """

if __name__ == "__main__":
    main()
