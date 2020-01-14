import json
import os

class Package():
    """
    Class representing a serialized upload package containing one or more batches.
    """

    def __init__(self, destination):
        self.root = destination
        self.batches = []
        if not os.path.exists(self.root):
            os.makedirs(self.root)

    def add(self, batch):
        self.batches.append(batch)

    def serialize_batches(self):
        for batch in self.batches:
            batch_path = os.path.join(self.root, "batches", batch.identifier)
            if not os.path.exists(batch_path):
                os.makedirs(batch_path)
            manifest_path = os.path.join(batch_path, "manifest.txt")
            with open(manifest_path, 'w') as handle:
                for asset in batch.assets:
                    handle.write(
                        f"{asset.restored.md5} {asset.restored.path}\n"
                        )
            duplicates_path = os.path.join(batch_path, "duplicates.txt")
            with open(duplicates_path, 'w') as handle:
                for dupe in batch.duplicates:
                    handle.write(f"{dupe}\n")

    def write_summary(self):
        data = {b.identifier: b.summary_dict() for b in self.batches}
        return json.dumps(data, indent=4, sort_keys=True)
