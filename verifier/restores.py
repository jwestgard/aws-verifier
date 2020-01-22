import sqlite3

class Database():

    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
    
    def __del__(self):
        self.connection.commit()
        self.connection.close()

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

    def lookup_batch(self, batch):
        query = """SELECT id FROM batches WHERE name=?;"""
        return self.cursor.execute(query, (batch.identifier,)).fetchall()

    def create_batch(self, batch):
        query = """INSERT INTO batches (name) VALUES (?);"""
        data = (batch.identifier,)
        id = self.cursor.execute(query, data).lastrowid
        if id:
            self.connection.commit()
            return id
        else:
            return None

    def create_asset(self, asset, source_id):
        cur = self.connection.cursor()
        query = """INSERT INTO assets 
                    (filename, md5, bytes, source_id, source_line)
                   VALUES (?, ?, ?, ?, ?)"""
        data = (asset.filename, asset.md5, asset.bytes, 
                source_id, asset.sourceline
                )
        cur.execute(query, data)
        return cur.lastrowid

    def create_dirlist(self, dirlist, batch_id):
        cur = self.connection.cursor()
        query = """INSERT INTO dirlists 
                    (filename, md5, bytes, batch_id)
                   VALUES (?, ?, ?, ?)"""
        data = (dirlist.filename, dirlist.md5, 
                dirlist.bytes, batch_id
                )
        cur.execute(query, data)
        return cur.lastrowid

    def lookup_dirlist_by_name(self, name):
        cur = self.connection.cursor()
        query = """SELECT id FROM dirlists WHERE filename=?;"""
        results = cur.execute(query, (name,)).fetchall()
        if len(results) == 1:
            return results[0][0]
        else:
            return None
        
    def create_instance(self, instance):
        cur = self.connection.cursor()
        query = """INSERT INTO instances 
                    (filename, md5, bytes, dirlist_id, dirlist_line)
                   VALUES (?, ?, ?, ?, ?)"""
        data = (instance.filename, instance.md5, instance.bytes,
                instance.dirlist_id, instance.dirlist_line)
        cur.execute(query, data)
        return cur.lastrowid


class Asset():
    pass


class Batch():
    pass


class Dirlist():
    pass


class Instance():
    pass


class RestoredAsset():

    def __init__(self, id, bytes, md5, filename, path):
        self.id = id
        self.bytes = bytes
        self.md5 = md5
        self.filename = filename
        self.path = path
