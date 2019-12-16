CREATE INDEX md5_lookup on files(md5);
CREATE INDEX filename_lookup on files(filename);
CREATE INDEX namesize_lookup on files(filename, bytes);