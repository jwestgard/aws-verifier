DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS dirlists;

CREATE TABLE files(
    uuid       TEXT PRIMARY KEY UNIQUE NOT NULL,
    bytes      INTEGER,
    md5        TEXT,
    filename   TEXT,
    path       TEXT,
    sourcefile INTEGER,
    sourceline INTEGER,
    action     TEXT,
    FOREIGN KEY(sourcefile) REFERENCES dirlists(id)
);

CREATE TABLE dirlists(
    id         INTEGER PRIMARY KEY UNIQUE NOT NULL,
    md5        TEXT,
    filename   TEXT,
    share      TEXT,
    batch      TEXT
);
