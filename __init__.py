import os
from shared import util
from shared.config import Config
from shared.di import injector
from shared.iconfig import IConfig

PROJECT_CACHE_ROOT = os.path.join(util.get_cache_root(), 'apollo')
PROJECT_DATA_ROOT = os.path.join(util.get_data_root(), 'apollo')

# injector.provide_instance(Config, Config(r'C:\dev\counterparty-risk-202206\risk-projection\src\main\python\riskprojection\config-20220916.capitolis.json'))
# injector.provide_instance(Config, Config(r'C:\mydev\counterparty-risk\risk-projection\src\main\python\riskprojection\config-20220913.capitolis.json'))
    # r'C:\dev\counterparty-risk-202206\risk-projection\src\main\python\riskprojection\config-20220913.capitolis.json'))
