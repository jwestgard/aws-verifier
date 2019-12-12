#!/user/bin/env python3

import csv
import os
import sys

CONFIGFILE = sys.argv[1]

def main():
    with open(CONFIGFILE) as handle:
        config = handle.read()
    print(config)


if __name__ == "__main__":
    main()
