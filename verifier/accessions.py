import csv
from datetime import datetime
import os
import re
import sys

from .utils import calculate_md5
from .utils import human_readable


class Asset():
    """
    Class representing a single asset under preservation.
    """

    def __init__(self, filename, sourcefile, sourceline,
                 bytes=None, timestamp=None, md5=None):
        self.filename = filename
        self.bytes = bytes
        self.timestamp = timestamp
        self.md5 = md5
        self.restored = []
        self.extra_copies = []
        self.sourcefile = sourcefile
        self.sourceline = sourceline
        self.status = 'Not checked'

    @property
    def signature(self):
        return (self.filename, self.md5, self.bytes)


class Batch():
    """
    Class representing a set of assets having been accessioned.
    """

    def __init__(self, identifier, *dirlists):
        self.identifier = identifier
        self.dirlists = [d for d in dirlists]
        self.assets = []
        self.status = None
        for dirlist in self.dirlists:
            self.load_assets(dirlist)

    @property
    def bytes(self):
        return sum(
            [asset.bytes for asset in self.assets if asset.bytes is not None]
            )

    @property
    def has_hashes(self):
        return all(
            [asset.md5 is not None for asset in self.assets]
            )

    def load_assets(self, dirlist):
        self.assets.extend([asset for asset in dirlist.assets])

    def summary_dict(self):
        return {'identifier': self.identifier,
                'dirlists': {d.md5: d.filename for d in self.dirlists},
                'num_assets': len(self.assets),
                'bytes': self.bytes,
                'human_readable': human_readable(self.bytes),
                'status': self.status
                }
    @property
    def asset_root(self):
        return os.path.commonpath([a.restored.path for a in self.assets])

    def has_duplicates(self):
        return len(self.assets) < len(set([a.signature for a in self.assets]))


class DirList():
    """
    Class representing an accession inventory list
    making up all or part of a batch.
    """

    def __init__(self, path):
        self.filename = os.path.basename(path)
        self.path = path
        self.bytes = int(os.path.getsize(path))
        self.md5 = calculate_md5(path)
        self.dirlines = 0
        self.extralines = 0
        self.lines = self.read()

    def read(self):
        for encoding in ['utf8', 'iso-8859-1', 'macroman']:
            try:
                with open(self.path, encoding=encoding) as handle:
                    return [line.strip() for line in handle.readlines()]
            except ValueError:
                continue
        print(f'Could not read directory listing file {self.path}')
        sys.exit(1)

    @property
    def assets(self):
        results = []
        # Examine the dirlist layout and set up iteration
        # Handle space-delimited dirlists
        ptrn = r'^(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s[AP]M)\s+([0-9,]+)\s(.+?)$'
        if self.lines[0].startswith('Volume in drive'):
            for n, line in enumerate(self.lines):
                # check if the line describes an asset
                match = re.match(ptrn, line)
                if not match:
                    continue
                else:
                    timestamp = datetime.strptime(match.group(1),
                                                 '%m/%d/%Y %I:%M %p'
                                                 )
                    bytes = int(''.join(
                        [c for c in match.group(2) if c.isdigit()])
                        )
                    filename = match.group(3)
                    results.append(
                        Asset(filename=filename, bytes=bytes,
                              timestamp=timestamp, sourcefile=self.filename,
                              sourceline=n)
                        )
            return results

        # Handle semi-colon separted dirlists
        elif ';' in self.lines[0]:
            for n, line in enumerate(self.lines):
                cols = line.split(';')
                if cols[2] == 'Directory':
                    continue
                else:
                    filename = os.path.basename(cols[0].rsplit('\\')[-1])
                    timestamp = datetime.strptime(cols[1],
                                                  '%m/%d/%Y %I:%M:%S %p'
                                                  )
                    bytes = round(float(cols[2].replace(',', '')) * 1024)
                    results.append(
                        Asset(filename=filename, bytes=bytes,
                              timestamp=timestamp, sourcefile=self.filename,
                              sourceline=n)
                        )
            return results

        # Handle CSV and TSV files
        else:
            delimiter = '\t' if '\t' in self.lines[0] else ','
            possible_keys = {
                'filename': ['Filename', 'File Name', 'FILENAME', 'Key',
                             '"Filename"', '"Key"'],
                'bytes': ['Size', 'SIZE', 'File Size', 'Bytes', 'BYTES',
                          '"Size"'],
                'timestamp': ['Mod Date', 'Moddate', 'MODDATE', '"Mod Date"'],
                'md5': ['MD5', 'Other', 'Data', '"Other"', '"Data"', 'md5']
                }
            columns = self.lines[0].split(delimiter)
            operative_keys = {}
            for attribute, keys in possible_keys.items():
                for key in keys:
                    if key in columns:
                        operative_keys[attribute] = key.replace('"','')
                        break
            reader = csv.DictReader(self.lines,
                                    quotechar='"',
                                    delimiter=delimiter
                                    )
            for n, row in enumerate(reader):
                # Skip extra rows in Prange-style "CSV" files
                if 'File Name' in row and any([
                    (row.get('Type') == 'Directory'),
                    (row.get('File Name').startswith('Extension')),
                    (row.get('File Name').startswith('Total file size')),
                    (row.get('File Name') == '')
                    ]):
                    continue
                else:
                    filename_key = operative_keys.get('filename')
                    if filename_key is not None:
                        filename = row[filename_key]
                    else:
                        filename = None

                    bytes_key = operative_keys.get('bytes')
                    if bytes_key is not None:
                        raw = row[bytes_key]
                        digits = ''.join([c for c in raw if c.isdigit()])
                        if digits is not '':
                            bytes = int(digits)
                        else:
                            bytes = None
                    else:
                        bytes = None

                    timestamp_key = operative_keys.get('timestamp')
                    if timestamp_key is not None:
                        timestamp = row[timestamp_key]
                    else:
                        timestamp = None

                    md5_key = operative_keys.get('md5')
                    if md5_key is not None:
                        md5 = row[md5_key]
                    else:
                        md5 = None
                    results.append(
                        Asset(filename=filename, bytes=bytes,
                              timestamp=timestamp, md5=md5,
                              sourcefile=self.filename, sourceline=n)
                        )
            return results
