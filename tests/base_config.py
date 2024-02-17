from abc import ABC
from os import getenv
from pathlib import Path

ROOT = Path(__file__).parent.absolute()


class LocalTestConfig(ABC):
    MINIO_PUBLIC_SERVER = getenv("MINIO_PUBLIC_SERVER")
    MINIO_ACCESS_KEY = getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = getenv("MINIO_SECRET_KEY")
