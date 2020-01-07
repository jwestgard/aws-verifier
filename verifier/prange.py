#!/usr/bin/env python3

import csv
import os
import re
import sqlite3
import sys


class Asset():

    def __init__(self, filename, md5, bytes):
        self.filename = filename
        self.md5 = md5
        self.bytes = int(bytes)

    def found(self, cursor):
        fb_query  = """SELECT * FROM files
                        WHERE filename=? and bytes=?;"""
        f_query   = """SELECT * FROM files
                        WHERE filename=?;"""
        fmb_query = """SELECT * FROM files
                        WHERE filename=? and md5=? and bytes=?;"""
        data = (self.filename, self.md5, self.bytes)
        result = cursor.execute(fmb_query, data).fetchall()
        if len(result) > 0:
            return True
        else:
            return False

class DirList():

    def __init__(self, path):
        self.path = path
        self.filename = os.path.basename(path)
        self.lines = self.read()
        self.assets = []
        self.assets_found = 0
        self.reported_bytes = None

    def bytes(self):
        return sum([asset.bytes for asset in self.assets])

    def read(self):
        encodings = ['utf-8', 'latin1', 'macroman']
        for encoding in encodings:
            try:
                with open(self.path, encoding=encoding) as handle:
                    self.lines = [line for line in handle.read().split('\n')]
                return
            except ValueError:
                continue
        sys.exit(f"Cannot decode file at {self.path}")

    def parse(self):
        csv_lines = []
        self.summary = {}
        for line in self.lines:
            if line == '':
                continue
            elif line.startswith('Extension'):
                marker, ext, count = line.split(' - ')
                self.summary[ext] = int(count.replace(',', ''))
            elif line.startswith('Total file size:'):
                match = re.match(r'Total file size: ([0-9,]+) bytes.', line)
                if match:
                    self.reported_bytes = int(match.group(1).replace(',', ''))
            else:
                csv_lines.append(line)

        for row in csv.DictReader(csv_lines, delimiter="\t"):
            if row['Type'] == "File":
                asset = Asset(row['File Name'], row['MD5'], row['File Size'])
                self.assets.append(asset)

    def display(self):
        print(self.filename.upper())
        print(f" LINES: {len(self.lines)}")
        print(f"ASSETS: {len(self.assets)}")
        print(f" FOUND: {self.assets_found}")
        print(f" BYTES: {self.bytes()}")
        print(f"RBYTES: {self.reported_bytes}")


def main():
    dbpath = ('/Users/westgard/Box Sync/'
              'AWSMigration/aws-migration-data/'
              'restored.db'
              )
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()
    outputfile = open(sys.argv[2], 'w')

    for dir, subdirs, files in os.walk(sys.argv[1]):
        for file in files:
            if file.endswith(".txt"):
                path = os.path.join(dir, file)
                print(f"\nProcessing {file}...")
                dirlist = DirList(path)
                dirlist.read()
                dirlist.parse()
                for asset in dirlist.assets:
                    if asset.found(cursor):
                        dirlist.assets_found += 1
                dirlist.display()
                row = [dirlist.filename,
                       len(dirlist.lines),
                       len(dirlist.assets),
                       dirlist.assets_found,
                       dirlist.bytes(),
                       dirlist.path
                       ]
                outputfile.write(','.join([str(i) for i in row]) + '\n')

    outputfile.close()

if __name__ == "__main__":
    main()
