#!/usr/bin/env python3

import csv
import os
import re
import sys

class RestoredFileList():
    """
    Class representing a list of restored assets in the form:
    md5, path, filename, size in btyes.
    """

    def __init__(self, path):
        self.filename = os.path.basename(path)
        self.batchname = self.filename[59:-14]
        self.id = self.get_identifier()
        with open(path) as handle:
            reader = csv.reader(handle)
            self.contents = []
            for row in reader:
                asset = Asset(*row)
                self.contents.append(asset)

    def size(self):
        """Return the sum of the bytes for all assets in the list."""
        return sum([asset.bytes for asset in self.contents])

    def get_identifier(self):
        """Attempt to extract an id of form 'batch_mdu_name' from batchname."""
        pattern = r'batch[ _]mdu[ _]\w+'
        match = re.search(pattern, self.batchname)
        if match:
            return match.group(0).replace(' ', '_')
        else:
            return "batch_mdu"


class Asset():
    """Class representing an individual asset record."""

    def __init__(self, md5, path, filename, bytes):
        self.md5 = md5
        self.path = path
        self.filename = filename
        self.bytes = int(bytes)
        

for file in os.listdir(sys.argv[1]):
    path = os.path.join(sys.argv[1], file)
    rfl = RestoredFileList(path)
    print(f"{rfl.id}\t{len(rfl.contents)}\t{rfl.size()}\t{rfl.filename}")
