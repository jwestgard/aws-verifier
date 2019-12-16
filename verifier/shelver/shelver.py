#!/usr/bin/env python3

import os
import shelve
import sys

from classes import InventoryFile
from classes import AccessionRecord

ROOT = sys.argv[1]
SHELF = sys.argv[2]


def main():
    if os.path.isfile(ROOT):
        paths = [ROOT]
    elif os.path.isdir(ROOT):
        paths = []
        for current, dirs, files in os.walk(ROOT):
            paths.extend([os.path.join(ROOT, current, f) for f in files])

    #with shelve.open(SHELF) as data:
    for filepath in paths:
        inv = InventoryFile(filepath)
        inv.show()
        print(len(inv.accessions))
        print(len([a for a in inv.accessions if a.filename.endswith('.tif')]))
        print(inv.directories)
        print([a.filename for a in inv.accessions if not a.filename.endswith('.tif')])


if __name__ == "__main__":
    main()
