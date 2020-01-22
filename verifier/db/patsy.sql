DROP TABLE IF EXISTS instances;
DROP TABLE IF EXISTS dirlists;
DROP TABLE IF EXISTS accessions;
DROP TABLE IF EXISTS accession_batches;

CREATE TABLE instances(
    uuid         TEXT PRIMARY KEY UNIQUE NOT NULL,
    filename     TEXT,
    md5          TEXT,
    bytes        INTEGER,
    dirlist_id   INTEGER,
    dirlist_line INTEGER,
    path         TEXT,
    action       TEXT,
    FOREIGN KEY(dirlist_id) REFERENCES dirlists(id)
);

CREATE TABLE dirlists(
    id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
    filename    TEXT,
    md5         TEXT,
    bytes       INTEGER,
    batch_id    INTEGER,
    FOREIGN KEY(batch_id) REFERENCES batches(id)
);

CREATE TABLE assets(
    id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
    filename    TEXT,
    md5         TEXT,
    bytes       INTEGER,
    source_id   INTEGER,
    source_line INTEGER,
    relpath     TEXT,
    FOREIGN KEY(source_id) REFERENCES dirlists(id)
);

CREATE TABLE batches(
    id          INTEGER PRIMARY KEY UNIQUE NOT NULL,
    name        TEXT
);

CREATE INDEX md5_lookup on instances(md5);
CREATE INDEX filename_lookup on instances(filename);
CREATE INDEX namesize_lookup on instances(filename, bytes);
