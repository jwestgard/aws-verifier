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

        # c. check for in-batch duplicate files
        print(f"  Has duplicate files: {batch.has_duplicates()}")

        # (4) Process assets
        print(f"  Processing batch assets...")
        assets_with_duplicates = []
        restores = []
        for n, asset in enumerate(batch.assets, 1):
            # a. skip excluded filenames
            if asset.filename in exclude_patterns:
                #print(  f"Excluding {asset.filename}!")
                asset.status = 'Deaccession'
                continue

            # b. skip invisible files
            if asset.filename.startswith('.'):
                #print(  f"Skipping invisible file {asset.filename}!")
                asset.status = 'Deaccession'
                continue

            # d. look for restored copies
            if asset.md5 is None and asset.bytes is None:
                matches = database.match_filename(asset)
            elif asset.md5 is None and asset.bytes is not None:
                matches = database.match_filename_bytes(asset)
            
            # if previous has failed, try simple filename match
            if not matches:
                matches = database.match_filename_bytes_md5(asset)

            if matches:
                asset.status = 'Found'
                if len(matches) == 1:
                    asset.restored = matches[0]
                    restores.append(matches[0].path)
                else:
                    asset.duplicates = matches
                    assets_with_duplicates.append(asset)
            else:
                asset.status = 'NotFound'
            
        # if nothing in batch was found, abort here
        if all([asset.status == 'NotFound' for asset in batch.assets]):
            print(f'  No Assets in this batch were found. Skipping...')
            continue
            
        # Add "best match" duplicates to transfer batch
        print(restores)
        common_path = os.path.commonpath(restores)
        print(f"  Common Path: {common_path}")
        for asset in assets_with_duplicates:
            for candidate in asset.duplicates:
                if candidate.path.startswith(common_path):
                    asset.restored = candidate
                    asset.duplicates.remove(candidate)
                    asset.extra_copies.extend(asset.duplicates)
                    break
                else:
                    asset.extra_copies.append(candidate)
            # if a relpath match is not found, use first duplicate
            if not asset.restored:
                asset.restored = asset.extra_copies.pop(0)
            print(asset.restored.path)
            print([f.path for f in asset.extra_copies])

        # Update batch status to reflect state of assets
        if all([a.status == 'Found' for a in batch.assets]):
            batch.status = 'Complete'
            package.add(batch)
            print(f"  Asset Root: {batch.asset_root}")

    # (5) Write out upload package
    print(f"Writing {len(package.batches)} batches to upload package...")
    for n, batch in enumerate(package.batches, 1):
        print(f"  ({n}) {batch.identifier} is complete and ready to load!")
    print(f"Writing package summary file...")
    with open(os.path.join(package.root, 'summary.json'), 'w') as handle:
        handle.write(package.write_summary())
    print(f"Writing package batch dirs...")
    package.serialize_batches()


if __name__ == "__main__":
    main()
