#!/usr/bin/env python3

import csv
import os

BATCHLIST = 'newspaper_batches.csv'
DIRLIST_ROOT = '/Users/westgard/Box Sync/AWSMigration/aws-migration-data/RestoredFilesEnhanced'

class Asset():
    def __init__(self, md5, path, filename, bytes):
        self.md5 = md5
        self.path = path
        self.filename = filename
        self.bytes = bytes


class DirList():
    def __init__(self, path):
        self.path = path
        self.contents = []
        with open(self.path) as handle:
            for linenumber, line in enumerate(handle, 1):
                asset = Asset(*line.strip().split(','))
                self.contents.append((linenumber, asset))

    def assetset(self):
        return frozenset([(a.md5, a.filename, a.bytes) for (n, a) in self.contents])


def main():
    with open(BATCHLIST) as handle:
        reader = csv.DictReader(handle)
        batches = {}
        for row in reader:
            key = (row['phase'], row['batch'])
            batches.setdefault(key, []).append(row)

    total_batches = sum([len(b) for b in batches.values()])

    for batch in batches:
        master_set = set()
        if batch[0] == 'phase3':
            continue
        else:
            print(f"\n{batch[1].upper()}: {len(batches[batch])} directories")
            dir_sets = []
            for n, dirlist in enumerate(batches[batch], 1):
                path = os.path.join(DIRLIST_ROOT, dirlist['inv_path'])
                d = DirList(path)
                dir_set = d.assetset()
                dir_sets.append((len(dir_set), n, path, dir_set))
                
            for ds in dir_sets:
                master_set = master_set.union(ds[3])

            print(f"  (M) {len(master_set)}")
            sorted_sets = sorted(dir_sets, reverse=True)
            largest_set = sorted_sets[0]
            print(f"  largest = {len(largest_set[3])}")
            for ds in sorted_sets[1:]:
                diff = ds[3] - largest_set[3]
                print(f"  ({ds[1]}) {ds[0]:5}: {len(diff)} elems not in largest")



if __name__ == "__main__":
    main()
