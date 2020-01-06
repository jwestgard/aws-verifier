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
            #print(f'Querying filename, md5, bytes...')
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
            #print(f'Querying filename, bytes...')
            data = (self.filename, self.bytes)
            results = cursor.execute(fb_query, data).fetchall()
        else:
            #print(f'Querying filename...')
            data = (self.filename,)
            results = cursor.execute(f_query, data).fetchall()
        # Evaluate the results
        return len(results), changes


class Batch():
    """
    Class representing a set of assets being preserved.
    """

    def __init__(self, name, date):
        self.name = name
        self.date = date
        self.assets = []
        self.dirlists = []
        self.accessions = []
        self.excluded = []
        self.missing = []
        self.changes = []

    def load_from(self, source, exclude_patterns):
        for asset in source.assets():
            if asset.filename in exclude_patterns:
                self.excluded.append(asset)
            else:
                self.assets.append(asset)

    def lookup_assets(self, cursor):
        total_assets = len(self.assets)
        matches = 0
        duplicates = 0
        not_found = 0
        for asset in self.assets:
            count, changes = asset.check_status(cursor)
            if changes:
                self.changes.extend(changes)
            if count == 0:
                print(f"{asset.filename} not found!")
                not_found += 1
            elif count == 1:
                matches += 1
            else:
                matches += 1
                duplicates += (count - 1)
        print((f"Total: {total_assets}, ",
               f"Matches: {matches}, ",
               f"Duplicates: {duplicates}, ",
               f"Not Found: {not_found}"))
        return (self.name, self.date, str(total_assets), 
                str(matches), str(duplicates), str(not_found))


    def create_reports(self, root):
        fieldnames = ['md5', 'filename', 'bytes', 'timestamp']
        batchdir = os.path.join(root, self.name)
        if not os.path.exists(batchdir):
            os.makedirs(batchdir)
        # Write the various categories of asset to their respective files
        for data, file in [
            (self.assets, 'accessions.csv'),
            (self.excluded, 'excludes.csv'),
            (self.missing, 'missing.csv')
            ]:
            path = os.path.join(batchdir, file)
            print(f'Writing to {path}...')
            with open(path, 'w') as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                for asset in data:
                    writer.writerow({'md5': asset.md5, 
                                     'filename': asset.filename,
                                     'bytes': asset.bytes, 
                                     'timestamp': asset.timestamp
                                     })


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





"""
    # check if the line describes a directory
    match = re.match(
        r'^(\d{2})/(\d{2})/(\d{4})\s+(\d{2}:\d{2}\s[AP]M)\s+(<DIR>)\s(.+?)$',
        line)
    if match:
        self.dirlines += 1
        continue
    # check if the line is the asset summary line
    match = re.match(r'^(\d+)\s+File\(s\)\s+([0-9,]+)\s+bytes$', line)
    if match:
        self.reported_assetcount = int(match.group(1).replace(',', ''))
        self.reported_bytes = int(match.group(2).replace(',', ''))
        self.extralines += 1
        continue
    # check if the line is the directory summary line
    match = re.match(r'^(\d+)\s+Dir\(s\)\s+([0-9,]+)\s+bytes\sfree', line)
    if match:
        self.reported_dircount = match.group(1)
        self.extralines += 1
        continue
    # otherwise add to extra line count
    self.extralines += 1
    #print(line)
#if len(assets) + self.dirlines + self.extralines == len(self.lines) and \ 
if len(self.assets) == self.reported_assetcount:
    print(len(self.assets), self.reported_assetcount)
    print(self.size(), self.reported_bytes)
    print('validated!')
else:
    print('not validated')
"""
