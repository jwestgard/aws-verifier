DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS dirlists;
DROP TABLE IF EXISTS accessions;
DROP TABLE IF EXISTS accessionBatches;

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

CREATE TABLE accessions(
    id         INTEGER PRIMARY KEY UNIQUE NOT NULL,
    filename   TEXT,
    bytes      INTEGER,
    md5        TEXT,
    sourcefile INTEGER,
    sourceline INTEGER,
    FOREIGN KEY(sourceFile) REFERENCES dirlists(id)
);

CREATE TABLE accession_batches(
    id         INTEGER PRIMARY KEY UNIQUE NOT NULL,
    name       TEXT,
    date       TEXT
);

CREATE INDEX md5_lookup on files(md5);
CREATE INDEX filename_lookup on files(filename);
CREATE INDEX namesize_lookup on files(filename, bytes);
