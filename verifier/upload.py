import os

class Package():
    """
    Class representing a serialized upload package.
    """

    def __init__(self, batch, desination):
        self.batch = batch
        self.name = batch.name
        self.path = os.path.join(desination, self.name)
        if not os.path.exists(self.path):
            os.makedir(self.path)
