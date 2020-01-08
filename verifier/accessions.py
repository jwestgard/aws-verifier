import csv
from datetime import datetime
import os
import re
import sys

from .utils import calculate_md5


class Asset():
    """
    Class representing a single asset under preservation.
    """

    def __init__(self, filename, bytes=None, timestamp=None, md5=None):
        self.filename = filename
        self.bytes = bytes
        self.timestamp = timestamp
        self.md5 = md5

    def check_status(self, cursor):
        changes = []
        fmb_query = """SELECT * FROM files
                        WHERE filename=? and md5=? and bytes=?;"""
        fb_query  = """SELECT * FROM files
                        WHERE filename=? and bytes=?;"""
        f_query   = """SELECT * FROM files
                        WHERE filename=?;"""
        if self.md5 is not None and self.bytes is not None:
            data = (self.filename, self.md5, self.bytes)
            results = cursor.execute(fmb_query, data).fetchall()
            if len(results) == 0:
                data = (self.filename, self.bytes)
                results = cursor.execute(fb_query, data).fetchall()
                if len(results) >= 1:
                    for result in results:
                        change = (self.filename, self.md5, result[2], result[4])
                        changes.append(change)
        elif self.bytes is not None:
            data = (self.filename, self.bytes)
            results = cursor.execute(fb_query, data).fetchall()
        else:
            data = (self.filename,)
            results = cursor.execute(f_query, data).fetchall()
        return len(results), changes


class DirList():
    """
    Class representing an accession inventory list making up all or part of a batch.
    """

    def __init__(self, path):
        self.filename = os.path.basename(path)
        self.path = path
        self.md5 = calculate_md5(path)
        self.dirlines = 0
        self.extralines = 0
        for encoding in ['utf8', 'iso-8859-1', 'macroman', 'windows-1252']:
            try:
                with open(path) as handle:
                    self.lines = [line.strip() for line in handle.readlines()]
            except ValueError:
                continue
        if not hasattr(self, 'lines'):
            print(f'Could not read directory listing file {self.path}')
            sys.exit(1)

    def size(self):
        return sum([asset.bytes for asset in self.assets])

    def assets(self):
        """
        Return a list of Asset objects for all valid accession records
        in the DirList
        """
        assets = []
        firstline = self.lines[0]

        # Handle dirlist-style files
        ptrn = r'^(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s[AP]M)\s+([0-9,]+)\s(.+?)$'
        if firstline.startswith('Volume in drive'):
            for n, line in enumerate(self.lines):
                # check if the line describes an asset
                match = re.match(ptrn, line)
                if match:
                    timestamp = datetime.strptime(
                                    match.group(1), '%m/%d/%Y %I:%M %p'
                                    )
                    bytes = int(
                        ''.join([c for c in match.group(2) if c.isdigit()])
                        )
                    filename = match.group(3)
                    asset = Asset(filename=filename,
                                  bytes=bytes,
                                  timestamp=timestamp
                                  )
                    assets.append(asset)

        # Handle semicolon-separated tabular files
        elif ';' in firstline:
            for line in self.lines:
                cols = line.split(';')
                if cols[2] == 'Directory':
                    self.dirlines += 1
                else:
                    filename = os.path.basename(cols[0].rsplit('\\')[-1])
                    timestamp = datetime.strptime(cols[1],
                                                  '%m/%d/%Y %I:%M:%S %p'
                                                  )
                    bytes = round(float(cols[2].replace(',', '')) * 1024)
                    asset = Asset(filename=filename,
                                  bytes=bytes,
                                  timestamp=timestamp
                                  )
                    assets.append(asset)

        # Handle CSV files
        else:
            if '\t' in firstline:
                delimiter = '\t'
            else:
                delimiter = ','
            mapping = {'filename':  ['Filename', 'File Name', 'FILENAME',
                                     'Key', '"Filename"', '"Key"'],
                       'bytes':     ['Size', 'SIZE', 'File Size', 'Bytes',
                                     'BYTES', '"Size"'],
                       'timestamp': ['Mod Date', 'Moddate', 'MODDATE',
                                     '"Mod Date"'],
                       'md5':       ['MD5', 'Other', 'Data', '"Other"',
                                     '"Data"']
            }
            columns = firstline.split(delimiter)
            lookup = {}
            for attribute, keys in mapping.items():
                for key in keys:
                    if key in columns:
                        lookup[attribute] = key.replace('"','')
                        break

            reader = csv.DictReader(self.lines,
                                    quotechar='"',
                                    delimiter=delimiter)
            for row in reader:
                # Skip rows in Prange-style "CSV" files
                if 'File Name' in row and any([
                    (row.get('Type') == 'Directory'),
                    (row.get('File Name').startswith('Extension')),
                    (row.get('File Name').startswith('Total file size')),
                    (row.get('File Name') == '')
                    ]):
                    continue
                else:
                    asset = Asset(
                        **{key: row[value] for key, value in lookup.items()}
                        )
                    assets.append(asset)
        return assets

