#!/usr/bin/env python3

import csv
import os
import sys

class RestoredFileList():
    """
    Class representing a list of restored assets in the form:
    md5, path, filename, size in btyes.
    """

    def __init__(self, path):
        self.filename = os.path.basename(path)
        self.batchname = self.filename[59:-14]
        print(self.batchname)

        with open(path) as handle:
            reader = csv.reader(handle)
            for row in reader:
                asset = Asset(*row)


class Asset():
    """Class representing an individual asset record"""

    def __init__(self, md5, path, filename, bytes):
        self.md5 = md5
        self.path = path
        self.filename = filename
        self.bytes = int(bytes)
        

for file in os.listdir(sys.argv[1])[:5]:
    path = os.path.join(sys.argv[1], file)
    rfl = RestoredFileList(path)
