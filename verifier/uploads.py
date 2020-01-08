import os

class Package():
    """
    Class representing a serialized upload package containing one or more batches.
    """

    def __init__(self, destination):
        self.root = destination
        if not os.path.exists(self.root):
            os.makedirs(self.root)

    def add_batch_folder(self, name):
        batch_path = os.path.join(self.root, "batches", name)
        if not os.path.exists(batch_path):
            os.makedirs(batch_path)