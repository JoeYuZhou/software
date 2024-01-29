# import configparser
import os
from shared.iconfig import IConfig
import copy
import json
from json_minify import json_minify
import getpass
from sys import platform


class BaseConfig(IConfig):
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.settings = BaseConfig.load_config(config_file_path)

    @staticmethod
    def load_config(config_file_path):
        import os
        import json
        import yaml

        ## define custom tag handler https://stackoverflow.com/questions/60055127/accept-anchor-alias-in-constructor-in-pyyaml
        def join(loader, node):
            seq = loader.construct_sequence(node)
            return "".join([str(i) for i in seq])

        yaml.add_constructor("!join", join)  ## register the tag handler

        def ospath(loader, node):
            seq = loader.construct_sequence(node)
            return os.path.join(*[str(i) for i in seq])

        yaml.add_constructor("!ospath", ospath)  ## register the tag handler

        def basicIsoDate(loader, node):
            # input = loader.construct_yaml_timestamp(node)
            # return input.strftime("%Y%m%d")
            input = loader.construct_sequence(node)
            return input[0].strftime("%Y%m%d")

        yaml.add_constructor("!basicIsoDate", basicIsoDate)  ## register the tag handler

        config_settings = None
        if os.path.exists(config_file_path):
            print(f"load_config config_file_path:{config_file_path}")
            file_name, file_extension = os.path.splitext(config_file_path)
            if file_extension == ".json":
                with open(config_file_path) as json_file:
                    # https://github.com/ultrajson/ultrajson (minifies the JSON by default)
                    # https://pypi.org/project/JSON_minify/
                    # https://gist.github.com/KinoAR/a5cf8a207529ee643389c4462ebf13cd
                    # config_settings = json.load( json_file) # JSON.parse(JSON.minify(my_str));
                    json_str = json_file.read()  # store file info in variable
                    config_settings = json.loads(json_minify(json_str))
            elif file_extension == ".yaml":
                with open(config_file_path, "r") as stream:
                    # config_settings = yaml.safe_load(stream)
                    config_settings = yaml.full_load(stream)
            else:
                raise TypeError("Uknown config extension")

        return config_settings

    def get(self, path):
        keys = BaseConfig.safe_path(path)
        selected = self.settings
        for key in keys:
            selected = selected[key]
        return selected

    def update(self, path, value):
        keys = BaseConfig.safe_path(path)
        current = self.settings
        for key in keys[:-1]:
            current = current[key]
        if keys[-1] in current and type(current[keys[-1]]) == dict:
            current[keys[-1]].update(value)
        else:
            current[keys[-1]] = value
        with open(self.config_file_path, "w") as fp:
            json.dump(self.settings, fp, indent=4)

    @staticmethod
    def safe_path(path):
        keys = path.split("/")
        return keys

    def path_exist(self, path):
        keys = BaseConfig.safe_path(path)
        current = self.settings
        for key in keys:
            if key not in current:
                return False
            current = current[key]
        return True

    def __getitem__(self, path):
        return self.get(path)

    def __setitem__(self, path, value):
        self.update(path, value)


class Config(BaseConfig):
    """
    Config consist
    config.ini which has the shared and default configuration among users
    {username}_config.ini (if exist) which overwrite/add key&value shared configuration for each user. such as var location, apollo app location
    """

    def __init__(self, user_config_file_path=None, comm_config_file_path=None):
        config_file_name = "config.json"
        if comm_config_file_path is None:
            # add linux support
            if "linux" in platform:
                config_file_name = "config_linux.json"

            comm_config_file_path = os.path.join(
                Config.get_file_path(), config_file_name
            )
        if user_config_file_path is None:
            user_config_name = f"{getpass.getuser()}_{config_file_name}"
            user_config_file_path = os.path.join(
                Config.get_file_path(), user_config_name
            )

        self.comm_config_file_path = comm_config_file_path
        self.user_config_file_path = user_config_file_path
        self.user_config = None
        self.comm_config = None
        self.config = self.load_config()

    @staticmethod
    def get_file_path():
        return os.path.dirname(os.path.realpath(__file__))

    def load_config(self):
        """
        1. load the default config
        2. check if user has its own config file.  overwrite (update & insert) config with new value if yes
        """
        comm_config = BaseConfig(self.comm_config_file_path)
        self.comm_config = comm_config

        config = copy.deepcopy(comm_config)

        user_config = BaseConfig(
            self.user_config_file_path
        )  # ?? JOE ?? inheritance or composition, not both!
        if user_config.settings is not None:
            config.settings.update(user_config.settings)
            self.user_config = user_config

        return config

    def update(self, path, value):
        """
        if user config exist
          if path exist in user config, update there
        elif path exist in common config, update there
        else insert into user config
        """

        if self.user_config is not None and self.user_config.path_exist(path):
            self.user_config[path] = value
        elif self.comm_config.path_exist(path):
            self.comm_config[path] = value

        elif self.user_config is not None:
            self.user_config[path] = value
        else:
            # create file
            user_config = BaseConfig(self.user_config_file_path)
            user_config.settings = {}
            user_config[path] = value

        self.config = self.load_config()

    def __getitem__(
        self, path
    ):  # ?? @todo fix the fact the client need to acesss the config field!
        return self.config.get(path)
