import csv
import datetime
import os
import re

from utils import md5checksum
from utils import sniff_encoding


class AccessionRecord():
    """A class representing the official accession record of an individual file."""

    def __init__(self, filename, sourcefile, sourceline, sourcehash, relpath, 
                 bytes=None, md5=None, mtime=None):
        self.filename = filename
        self.sourcefile = sourcefile
        self.sourceline = sourceline
        self.sourcehash = sourcehash
        self.relpath = relpath
        self.bytes = bytes
        self.md5 = md5
        self.mtime = mtime

    def show(self):
        for key in ['filename', 'sourcefile', 'sourceline', 'sourcehash', 'relpath',
                    'bytes', 'md5', 'mtime']:
            print(f'{key.upper(): >15}: {getattr(self, key)}')


class InventoryFile():
    """Class representing a text file containing a set of accession records."""

    def __init__(self, path):
        self.path = path
        self.filename = os.path.basename(path)
        self.encoding = sniff_encoding(self.path)
        self.md5 = md5checksum(self.path)
        with open(self.path, encoding=self.encoding) as handle:
            self.lines = [
                (n, line.rstrip('\n')) for n, line in enumerate(handle.readlines(), 1)
                ]
        self.type = self.sniff_format()
        self.directories = []
        self.excludes = []
        self.accessions = []
        self.read_accessions()

    def show(self):
        print(f'PATH: {self.path}')
        print(f'FILE: {self.filename}')
        print(f' MD5: {self.md5}')
        print(f'TYPE: {self.type}')
        print(f'ACCESSIONS: {len(self.accessions)}')
        for n, accession in enumerate(self.accessions, 1):
            print(f'\n({n}) {accession.filename}'.upper())
            accession.show()

    def sniff_format(self):
        head = self.lines[0][1]
        if 'Volume in drive' in head:
            return 'dirlist-a'
        elif ';' in head:
            return 'dirlist-b' 
        elif '\t' in head:
            return 'tsv'
        elif ',' in head:
            return 'csv'

    def raw_lines(self):
        return [line_tuple[1] for line_tuple in self.lines]

    def read_accessions(self):

        if self.type == 'dirlist-a':
            """
            This section reads text files in the columnar directory listing type,
            characterized by a summary of files, directories, and bytes at the end, and by
            non-asset-listing lines being indented by two spaces. This format was used for 
            the earliest DCR dirlists.
            """
            for linenumber, line in self.lines:
                if line.startswith(' ') or line == '':
                    self.excludes.append(line)
                else:
                    parts = line.split()
                    if parts[3] == '<DIR>':
                        self.directories.append(line)
                    else:
                        timestamp = datetime.datetime.strptime(
                                        ' '.join(parts[:3]), '%m/%d/%Y %I:%M %p'
                                        ).isoformat()
                        bytes     = int(parts[3].replace(',', ''))
                        filename  = parts[4]
                        accession = AccessionRecord(filename=filename, 
                                                    sourcefile=self.filename, 
                                                    sourceline=linenumber, 
                                                    sourcehash=self.md5,
                                                    bytes=bytes,
                                                    relpath='.',
                                                    mtime=timestamp
                                                    )
                        self.accessions.append(accession)

        elif self.type == 'tsv' or self.type == 'csv':
            """
            This section handles Prange's tabular directory listing format.
            """
            if self.type == 'tsv':
                delimiter = '\t'
            elif self.type == 'csv':
                delimiter = ','
            
            for linenumber, line in self.lines[1:]:
                cols = line.split('\t')
                if len(cols) <= 1:
                    self.excludes.append(line)
                elif cols[5] == 'Directory' or cols[0] == 'Thumbs.db':
                    self.directories.append(line)
                else:
                    filename  = cols[0]
                    bytes     = cols[2]
                    relpath   = cols[3]
                    timestamp = cols[4]
                    md5       = cols[6]
                    sha1      = cols[7]
                    accession = AccessionRecord(filename=filename, 
                                                sourcefile=self.filename, 
                                                sourceline=linenumber, 
                                                sourcehash=self.md5,
                                                bytes=bytes,
                                                relpath=relpath,
                                                mtime=timestamp
                                                )
                    self.accessions.append(accession)

        elif self.type == '':
            pass

        elif self.type == '':
            pass

        else:
            pass









