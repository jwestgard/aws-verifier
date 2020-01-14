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
    package_root = os.path.join(config['ROOTDIR'], config['OUTPUTDIR'])
    source_root = os.path.join(config['ROOTDIR'], config['SOURCEDIR'])
    database = Database(os.path.join(config['ROOTDIR'], config['DATABASE']))
    package = Package(package_root)

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
                                f"{asset.md5} {asset.restored.path} {dupe.path}"
                                )
                else:
                    asset.status = 'NotFound'

        # Update batch status to reflect state of assets
        complete_statuses = ['PerfectMatch', 'WithDuplicates']
        if all([a.status == 'PerfectMatch' for a in batch.assets]):
            batch.status = 'Complete'
            print('perfect match!')
            package.add(batch)
        elif all([a.status in complete_statuses for a in batch.assets]):
            batch.status = 'WithDuplicates'
            print('complete with duplicates!')
            package.add(batch)
        else:
            print()

    # (5) Write out upload package
    print(f"Writing {len(package.batches)} batches to upload package...")
    for n, batch in enumerate(package.batches, 1):
        print(f"  ({n}) {batch.identifier} is complete and ready to load!")
    print(f"Writing package summary file..")
    with open(os.path.join(package.root, 'summary.json'), 'w') as handle:
        handle.write(package.write_summary())


if __name__ == "__main__":
    main()
