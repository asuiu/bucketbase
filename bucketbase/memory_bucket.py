from pathlib import PurePosixPath
from threading import RLock
from typing import Iterable, Union

from streamerate import slist, sset, stream

from bucketbase import DeleteError
from bucketbase.ibucket import ShallowListing, IBucket


class MemoryBucket(IBucket):
    """
    Implements IObjectStorage interface, but stores all objects in memory.
    This class is intended to be used for testing purposes only.
    """

    def __init__(self) -> None:
        self._objects = {}  # Store files
        self._lock = RLock()

    def put_object(self, object_name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        _object_name = self._validate_name(object_name)

        _content = self._encode_content(content)
        with self._lock:
            self._objects[_object_name] = _content

    def get_object_content(self, object_name: PurePosixPath) -> bytes:
        _object_name = self._validate_name(object_name)

        with self._lock:
            if _object_name not in self._objects:
                raise FileNotFoundError(f"Object {_object_name} not found in MemoryObjectStore")
            return self._objects[_object_name]

    def list_objects(self, prefix: PurePosixPath) -> slist[PurePosixPath]:
        self._split_prefix(prefix)  # validate prefix
        str_prefix = str(prefix)
        with self._lock:
            result = stream(self._objects).filter(lambda obj: str(obj).startswith(str_prefix)).map(PurePosixPath).to_list()
        return result

    def shallow_list_objects(self, prefix: PurePosixPath) -> ShallowListing:
        self._split_prefix(prefix)  # validate prefix
        str_prefix = str(prefix)
        pref_len = len(str_prefix)
        objects = slist()
        prefixes = sset()
        with self._lock:
            for sobj in self.list_objects(prefix).map(str):
                if "/" not in sobj[pref_len:]:
                    objects.append(PurePosixPath(sobj))
                else:
                    suffix = sobj[pref_len:]
                    common_suffix = suffix.split("/", 1)[0]
                    common_prefix = str_prefix + common_suffix + "/"
                    prefixes.add(common_prefix)
        return ShallowListing(objects=objects, prefixes=prefixes.to_list())

    def exists(self, object_name: PurePosixPath | str) -> bool:
        _object_name = self._validate_name(object_name)
        with self._lock:
            return _object_name in self._objects

    def remove_objects(self, list_of_objects: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        _list_of_objects = [str(obj) for obj in list_of_objects]

        delete_errors = slist()
        with self._lock:
            for obj in _list_of_objects:
                obj = self._validate_name(obj)
                if obj in self._objects:
                    self._objects.pop(obj)
        return delete_errors
