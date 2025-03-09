try:
    from tests.local_config import LocalTestConfig
except ImportError:
    from tests.base_config import LocalTestConfig
CONFIG = LocalTestConfig
