import sqlite3

class Database():

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()

    def match_filename_bytes_md5(self, asset):
        query = """
            SELECT * FROM files WHERE filename=? and md5=? and bytes=?;
            """
        signature = (asset.filename, asset.md5, asset.bytes)
        results = self.cursor.execute(query, signature).fetchall()
        if results:
            return [RestoredAsset(*r[:5]) for r in results]
        else:
            return None

    def match_filename_bytes(self, asset):
        query = """SELECT * FROM files WHERE filename=? and bytes=?;"""
        signature = (asset.filename, asset.bytes)
        results = self.cursor.execute(query, signature).fetchall()
        if results:
            return [RestoredAsset(*r[:5]) for r in results]
        else:
            return None

    def match_filename(self, asset):
        query = """SELECT * FROM files WHERE filename=?;"""
        signature = (asset.filename,)
        results = self.cursor.execute(query, signature).fetchall()
        if results:
            return [RestoredAsset(*r[:5]) for r in results]
        else:
            return None


class RestoredAsset():

    def __init__(self, id, bytes, md5, filename, path):
        self.id = id
        self.bytes = bytes
        self.md5 = md5
        self.filename = filename
        self.path = path
