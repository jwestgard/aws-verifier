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
    database = Database(os.path.join(config['ROOTDIR'], config['DATABASE']))
    package = Package(os.path.join(config['ROOTDIR'], config['OUTPUTDIR']))


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
        print(f"Querying database for copies of assets...", end='')
        for n, asset in enumerate(batch.assets, 1):
            if asset.md5 is not None:
                matches = database.match_filename_bytes_md5(asset)
                if matches is not None:
                    if len(matches) == 1:
                        asset.status = 'PerfectMatch'
                        asset.restored = matches[0]
                    elif len(matches) > 1:
                        asset.status = 'WithDuplicates'
                        asset.restored = matches[0]
                        for dupe in matches[1:]:
                            batch.duplicates.append(
                                f"{asset.md5} {asset.restored.path} {dupe.path}")
                else:
                    asset.status = 'NotFound'

        if all([a.status == 'PerfectMatch' for a in batch.assets]):
            batch.status = 'Complete'
            print('perfect match!')
            package.add(batch)
        elif all([a.status in ('PerfectMatch', 'WithDuplicates') for a in batch.assets]):
            batch.status = 'WithDuplicates'
            print('complete with duplicates!')
            package.add(batch)
        else:
            print()

    # (5) Write out upload package
    print(f"Writing {len(package.batches)} complete batches to upload package...")
    for batch in package.batches:
        print(f"{batch.identifier} is complete and ready to load!")
    package.write_batches()

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
