import json
import os
import yaml

class Package():
    """
    Class representing a serialized upload package 
    containing one or more batches.
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
            #print(batch_path)
            if not os.path.exists(batch_path):
                os.makedirs(batch_path)
            manifest = open(os.path.join(batch_path, "manifest.txt"), 'w')
            deaccess = open(os.path.join(batch_path, "deaccessions.txt"), 'w')
            missing  = open(os.path.join(batch_path, "missing.txt"), 'w')
            for asset in batch.assets:
                if asset.status == 'Found':
                    manifest.write(f"{asset.md5} {asset.restored.path}\n")
                    for extra_copy in asset.extra_copies:
                        deaccess.write(
                            f"duplicate {asset.md5} {extra_copy.path}\n"
                            )
                elif asset.status == 'Deaccession':
                    deaccess.write(
                        f"deaccession {asset.md5} {asset.path}\n"
                        )
                elif asset.status == 'NotFound':
                    missing.write(
                        f"{asset.md5} {asset.bytes} {asset.filename}\n"
                        )
            manifest.close()
            deaccess.close()
            missing.close()
            self.write_master_package_yaml(
                os.path.join(self.root, 'batches.yml')
                )

    def write_master_package_yaml(self, path):
        data = {'batches_dir': 'batches',
                'batches': []
                }
        for batch in self.batches:
            data['batches'].append({'path': batch.identifier,
                                    'asset_root': batch.asset_root,
                                    'bucket': ''})
        with open(path, 'w') as handle:
            yaml.dump(data, handle, indent=4)

    def write_summary(self):
        data = {b.identifier: b.summary_dict() for b in self.batches}
        return json.dumps(data, indent=4, sort_keys=True)
