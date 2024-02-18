import io
from pathlib import Path, PurePosixPath
from typing import Iterable, Union

from streamerate import slist

from bucketbase.errors import DeleteError
from bucketbase.fs_bucket import AppendOnlyFSBucket
from bucketbase.ibucket import ShallowListing, IBucket


class CachedImmutableBucket(IBucket):
    def __init__(self, cache: IBucket, main: IBucket) -> None:
        self._cache = cache
        self._main = main

    def get_object(self, name: PurePosixPath | str) -> bytes:
        try:
            return self._cache.get_object(name)
        except FileNotFoundError:
            _content = self._main.get_object(name)
            self._cache.put_object(name, _content)
            return _content

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        raise io.UnsupportedOperation("put_object is not supported for CachedImmutableMinioObjectStorage")

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        return self._main.list_objects(prefix)

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        return self._main.shallow_list_objects(prefix)

    def exists(self, name: PurePosixPath | str) -> bool:
        return self._cache.exists(name) or self._main.exists(name)

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        raise io.UnsupportedOperation("remove_objects is not supported for CachedImmutableMinioObjectStorage")

    @classmethod
    def build_from_fs(cls, cache_root: Path, main: IBucket) -> "CachedImmutableBucket":
        cache_bucket = AppendOnlyFSBucket.build(cache_root)
        return CachedImmutableBucket(cache=cache_bucket, main=main)
