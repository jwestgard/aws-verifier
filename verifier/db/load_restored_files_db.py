#!/usr/bin/env python3

import csv
import hashlib
import os
import re
import sqlite3
import sys
import uuid


SEARCH_ROOT = "/Users/westgard/Box Sync/AWSMigration" + \
              "/aws-migration-data/RestoredFilesEnhanced/"

shares = [
    {'name':   'henson',
     'share':  'LIBRJimHensonShare-md5sum',
     'prefix': 'nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-LIBRJimHenson-Export_',
     'suffix': '_work_scan.csv',
     'bucket': 'libdc'
    },
    {'name':   'football',
     'share':  'LIBRFootballFilmsShare-md5sum',
     'prefix': 'nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-FootballFilms-Export_',
     'suffix': '_work_scan.csv',
     'bucket': 'libdc'
    },
    {'name':   'prange',
     'share':  'LIBRPrangeShare-md5sum',
     'prefix': 'nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-Prangeexport_',
     'suffix': '_work_scan.csv',
     'bucket': 'prange'
    },
    {'name':   'dcrprojects',
     'share':  'LIBRDCRProjectsShare-md5sum',
     'prefix': 'nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-Projects-Export_',
     'suffix': '_work_scan.csv',
     'bucket': 'libdc'
    },
    {'name':   'newspapers',
     'share':  'LIBRNewsPaperShare-md5sum',
     'prefix': 'nfs.isip01.nas.umd.edu_ifs_data_LIBR-Archives-Paper-Export_',
     'suffix': '_work_scan.csv',
     'bucket': 'newspapers'
    }
]


def calculate_md5(path):
    """
    Calclulate and return the object's md5 hash.
    """
    hash = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            data = f.read(8192)
            if not data:
                break
            else:
                hash.update(data)
    return hash.hexdigest()


def human_readable(bytes):
    for n, label in enumerate(['bytes', 'KiB', 'MiB', 'GiB', 'TiB']):
        value = bytes / (1024 ** n)
        if value < 1024:
            return f'{round(value, 2)} {label}'
        else:
            continue


class ShareDirectory():
    """
    Class representing a group of restored filelistings on one share volume.
    """

    def __init__(self, name, share, prefix, suffix, bucket):
        self.name   = name
        self.share  = share
        self.prefix = prefix
        self.suffix = suffix
        self.bucket = bucket


class RestoredFileList():
    """
    Class representing a list of restored assets in the form:
    md5, path, filename, size in btyes.
    """

    def __init__(self, path, share, cursor):
        self.filename = os.path.basename(path)
        self.share    = share.name
        self.prefix   = share.prefix
        self.suffix   = share.suffix
        self.bucket   = share.bucket
        self.md5      = calculate_md5(path)

        # Extract the label attched to the batch at transfer time
        if self.filename.startswith(self.prefix) and self.filename.endswith(self.suffix):
            self.batchname = self.filename[len(self.prefix):-len(self.suffix)]

        # Attempt to extract a batch identifier from the label
        self.batch = self.get_batch_identifier()

        # Create a record for the filelist in the database
        self.id = self.insert(cursor)

        # Read the contents of the file and create asset objects
        with open(path) as handle:
            reader = csv.reader(handle)
            self.contents = []
            for n, row in enumerate(reader, 1):
                asset = Asset(n, self.id, *row)
                self.contents.append(asset)

    def insert(self, cursor):
        data = (self.md5, self.filename, self.share, self.batch)
        query = '''INSERT INTO dirlists (md5, filename, share, batch) 
                     VALUES (?, ?, ?, ?);'''
        cursor.execute(query, data)
        return cursor.lastrowid

    def size(self):
        """Return the sum of the bytes for all assets in the list."""
        return sum([asset.bytes for asset in self.contents])

    def get_batch_identifier(self):
        """Attempt to extract an id based on conventions for each share."""
        if self.share == 'newspapers':
            pattern = r'batch[ _]mdu[ _]\w+'
            match = re.search(pattern, self.batchname)
            if match:
                return match.group(0).replace(' ', '_')
            else:
                return "batch_mdu_?"
        elif self.share == 'dcrprojects':
            pattern = r'DCR_Projects_Archive\d\d\d'
            match = re.search(pattern, self.batchname)
            if match:
                return match.group(0)
            else:
                return 'DCR_Projects_Archive???'
        elif self.share == 'prange':
            if 'General' in self.batchname:
                return 'General Books'
            else:
                return 'Children\'s Books'
        elif self.share == 'football':
            return 'Football_Archive001'
        elif self.share == 'henson':
            return 'Henson_Archive001'


class Asset():
    """Class representing an individual asset record."""

    def __init__(self, sourceline, sourcefile, md5, path, filename, bytes):
        self.sourceline = sourceline
        self.sourcefile = sourcefile
        self.md5        = md5
        self.path       = path
        self.filename   = filename
        self.bytes      = int(bytes)
        self.uuid       = str(uuid.uuid4())

    def insert(self, cursor):
        data = (self.uuid, self.bytes, self.md5, self.filename, self.path,
                self.sourceline, self.sourcefile)
        query = '''INSERT INTO files (uuid, bytes, md5, filename, 
                                      path, sourceline, sourcefile) 
                        VALUES (?, ?, ?, ?, ?, ?, ?);'''
        cursor.execute(query, data)
        return cursor.lastrowid


def generate_overview_report(shares):
    totaldirs = 0
    stats = []
    for item in shares:
        totalfiles = 0
        totalbytes = 0
        share = ShareDirectory(**item)
        #print(f'{share.name}, {share.share}')
        sharepath = os.path.join(SEARCH_ROOT, share.share)
        #print(f'Processing {sharepath}')
        for f in os.listdir(sharepath):
            totaldirs += 1
            filepath = os.path.join(sharepath, f)
            r = RestoredFileList(filepath, share)
            totalfiles += len(r.contents)
            totalbytes += r.size()
            print(
                f"{totaldirs:4}",
                f"{r.bucket:12}",
                f"{r.id:25}",
                f"{len(r.contents):8}",
                f"{r.size():16}",
                f"'{r.batchname}'"
            )
        stats.append((share.name, totalfiles, totalbytes, human_readable(totalbytes)))

    totalfiles = 0
    totalbytes = 0
    for share in stats:
        totalfiles += share[1]
        totalbytes += share[2]
        print(','.join([str(i) for i in share]))

    print(f'Total Bytes: {totalbytes} {human_readable(totalbytes)}')
    print(f'Total Files: {totalfiles}')


def main():
    # establish database connection
    total_deposits = 0
    db = sys.argv[1]
    con = sqlite3.connect(db)
    ins = '''INSERT INTO files(uuid, bytes, md5, filename, path, sourceline, sourcefile)         
                VALUES (?, ?, ?, ?, ?, ?, ?);'''

    # process each main directory
    for item in shares:
        share = ShareDirectory(**item)
        sharepath = os.path.join(SEARCH_ROOT, share.share)
        # process each dirlist
        for f in os.listdir(sharepath):
            filepath = os.path.join(sharepath, f)
            print(filepath)
            r = RestoredFileList(filepath, share, con.cursor())
            
            with con:
                data = [(a.uuid, a.bytes, a.md5, a.filename, a.path, a.sourceline,
                            a.sourcefile) for a in r.contents]
                con.executemany(ins, data)

    con.close()

if __name__ == "__main__":
    main()