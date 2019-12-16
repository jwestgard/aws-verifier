#!/usr/bin/env python3

import csv
import os
import sys

BATCH_INDEX = sys.argv[1]
INV_ROOT = '../../../Box Sync/AWSMigration/aws-migration-data/RestoredFilesEnhanced'

class Asset():

    def __init__(self, md5, path, filename, bytes):
        self.md5 = md5
        self.path = path
        self.filename = filename
        self.bytes = bytes


class DirList():

    def __init__(self, path):
        with open(path) as handle:
            for line in handle:
                asset = Asset(*line.strip().split(','))


class Batch():

    def __init__(self, name, dirs):
        self.name = name
        self.root = f'batch_mdu_{self.name}'
        self.dirs = dirs


def main():
    batches = {}
    with open(BATCH_INDEX) as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            path = os.path.join(INV_ROOT, row['inv_path'])
            batches.setdefault(row['batch'], []).append(DirList(path))

    for name, dirs in batches.items():
        batch = Batch(name, dirs)


if __name__ == "__main__":
    main()