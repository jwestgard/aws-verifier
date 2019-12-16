import hashlib


def sniff_encoding(path):
    """Attempt to sniff and return the file's encoding.""" 
    encodings = ['utf-8', 'latin-1']
    for encoding in encodings:
        try:
            f = open(path, encoding=encoding)
            f.read()
            f.close
            return encoding
        except ValueError:
            continue
    return None


def md5checksum(path):
    """Calculate the MD5 checksum of the provided file."""
    with open(path, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            else:
                m.update(data)
    return m.hexdigest()
