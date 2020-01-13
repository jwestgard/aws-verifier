import sqlite3

class Database():

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()

    def best_matches_for(self, asset):
        # Lookup by full asset signature
        if asset.md5 is not None and asset.bytes is not None:
            query = """SELECT * FROM files 
                         WHERE filename=? and md5=? and bytes=?;"""
            signature = (asset.filename, asset.md5, asset.bytes)
        # Lookup by filename and bytes
        elif asset.md5 is None and asset.bytes is not None:
            query = """SELECT * FROM files
                         WHERE filename=? and bytes=?;"""
            signature = (asset.filename, asset.bytes)
        # Lookup by filename only
        else:
            query = """SELECT * FROM files
                         WHERE filename=?;"""
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
