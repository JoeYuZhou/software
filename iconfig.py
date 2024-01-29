class IConfig(object):
    """
    all methods of this interface should be overwritten. if they are called
    directly,that would mean something is wrong and an exception should be thrown.
    """
    def __getitem__(self, path):
        raise NotImplementedError()

    def __setitem__(self, path, value):
        raise NotImplementedError()

    # pass
