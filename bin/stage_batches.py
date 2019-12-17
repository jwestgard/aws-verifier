#!/usr/bin/env python3

import csv
import os
import sys

INV_ROOT = '../../../Box Sync/AWSMigration/aws-migration-data/RestoredFilesEnhanced'

BATCH_INDEX = sys.argv[1]
OUTPUT_ROOT = sys.argv[2]


class Asset():

    def __init__(self, md5, abspath, relpath, filename, bytes, linenumber):
        self.md5 = md5
        self.abspath = abspath
        self.relpath = relpath
        self.filename = filename
        self.bytes = int(bytes)
        self.linenumber = linenumber


class DirList():

    def __init__(self, dirpath, batchroot):
        self.filename = os.path.basename(dirpath)
        self.transfers = []
        self.excludes = []
        self.duplicates = []
        self.changed = []
        with open(dirpath) as handle:
            for n, line in enumerate(handle, 1):
                md5, abspath, filename, bytes = line.strip().split(',')
                exmarkers = ['$RECYCLE.BIN', 'Preservation/dvv', 
                             'System Volume Information', 'Thumbs.db', '.DS_Store']
                if batchroot not in abspath or any([ex in abspath for ex in exmarkers]):
                    self.excludes.append((md5, abspath, filename, bytes))
                else:
                    start = abspath.rfind(batchroot)
                    nextslash = abspath.find('/', start)
                    relpath = abspath[(nextslash + 1):]
                    asset = Asset(md5, abspath, relpath, filename, bytes, n)
                    self.transfers.append(asset)

    def savings(self):
        ex = sum([int(a[3]) for a in self.excludes])
        du = sum([a.bytes for a in self.duplicates])
        return ex + du

class Batch():

    def __init__(self, name, dirpaths):
        self.name = name
        self.root = f'batch_mdu_{self.name}'
        self.dirlists = sorted([DirList(path, self.root) for path in dirpaths], 
                                    key=lambda x: -len(x.transfers))
        self.outpath = os.path.join(OUTPUT_ROOT, self.name)
        if not os.path.isdir(self.outpath):
            os.makedirs(self.outpath)

    def unique_assets(self):
        results = {}
        for dir in self.dirlists:
            for asset in dir.transfers:
                key = (asset.md5, asset.relpath, asset.bytes)
                results.setdefault(key, []).append(asset.abspath)
        return results

    def master_dirlist(self):
        largestdir = self.dirlists[0]
        return {asset.relpath: asset for asset in largestdir.transfers}


def main():
    batches = {}
    bytes_saved = 0
    files_saved = 0
    with open(BATCH_INDEX) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row['phase'] == 'phase3':
                continue
            else:
                abspath = os.path.join(INV_ROOT, row['inv_path'])
                batches.setdefault(row['batch'], []).append(abspath)

    for batchname, dirs in batches.items():
        print(f"\nProcessing {batchname.upper()}...")
        batch = Batch(batchname, dirs)
        lookup = batch.master_dirlist()

        for dir in batch.dirlists[1:]:
            for asset in dir.transfers:
                if asset.relpath not in lookup:
                    print(f"  - {asset.relpath} NOT FOUND! Adding...")
                    lookup[asset.relpath] = asset
                else:
                    target = lookup[asset.relpath]
                    if (asset.md5, asset.bytes) == (target.md5, target.bytes):
                        dir.duplicates.append(asset)
                    else:
                        dir.changed.append(asset)

        for n, dir in enumerate(batch.dirlists, 1):
            print(f" ({n}) {len(dir.excludes)} excludes")
            print(f"       {len(dir.duplicates)} duplicates")
            print(f"       {len(dir.changed)} changed")
            base = os.path.join(batch.outpath, dir.filename)
            if dir.excludes:
                with open(base + '_excludes.txt', 'w') as outhandle:
                    for md5, abspath, filename, bytes in dir.excludes:
                        outhandle.write(
                            f"{md5}\t{abspath}\t{filename}\t{bytes}\n"
                            )
            if dir.duplicates:
                with open(base + '_duplicates.txt', 'w') as outhandle:
                    for a in dir.duplicates:
                        outhandle.write(
                            f"{a.md5}\t{a.abspath}\t{a.filename}\t{a.bytes}\n"
                            )
            if dir.changed:
                with open(base + '_changed.txt', 'w') as outhandle:
                    for a in dir.changed:
                        outhandle.write(
                            f"{a.md5}\t{a.abspath}\t{a.filename}\t{a.bytes}\n"
                            )
            print(dir.savings())
            files_saved += len(dir.excludes + dir.duplicates)
            bytes_saved += dir.savings()

        with open(os.path.join(batch.outpath, 'transfer.txt'), 'w') as handle:
            for asset in sorted(lookup.values(), key=lambda a: a.abspath):
                handle.write(f"{asset.md5}\t{asset.abspath}\n")

    print(files_saved, bytes_saved)


if __name__ == "__main__":
    main()