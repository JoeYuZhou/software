from shared.config import Config

from shared import singleton


@singleton
class GlobalConfig:
    """
    single instance of global configuration shared for multiple projects
    for values such as "var" location,
    """

    def __init__(self):
        self.config = Config().config
