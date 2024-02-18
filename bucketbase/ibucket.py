import io
import os
import re
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import PurePosixPath, Path
from typing import Tuple, Optional, Union, Iterable

from pyxtension import PydanticValidated, validate
from pyxtension.models import ImmutableExtModel
from streamerate import slist

from bucketbase.errors import DeleteError

S3_NAME_CHARS_NO_SEP = r"\w!\-\.')("
S3_NAME_SAFE_RE = rf"^[{S3_NAME_CHARS_NO_SEP}][{S3_NAME_CHARS_NO_SEP}/]+$"


@dataclass(frozen=True)
class ShallowListing(ImmutableExtModel):
    objects: slist[PurePosixPath]
    prefixes: slist[str]


class IBucket(PydanticValidated, ABC):
    """
    This class is intended to be a base class for all object storage implementations.
    - it should not have any minio specific code
    - it should use only PurePosixPath as the object_name
    - it should not use bucket concept as it is not applicable to all object storage implementations.
        - Every instance of the this class will be associated with a single bucket for the lifetime of the instance.
    - No retries to the underlying storage (like Minio) can be used, since this should be done by the underlying Minio client
    """

    SEP = "/"
    SPLIT_PREFIX_RE = re.compile(rf"^((?:[{S3_NAME_CHARS_NO_SEP}]+/)*)([{S3_NAME_CHARS_NO_SEP}]*)$")
    OBJ_NAME_RE = re.compile(rf"^(?:[{S3_NAME_CHARS_NO_SEP}]+/)*[{S3_NAME_CHARS_NO_SEP}]+$")
    DEFAULT_ENCODING = "utf-8"
    MINIO_PATH_TEMP_SUFFIX_LEN = 43  # Minio will add to any downloaded path a `stat.etag + '.part.minio'` suffix
    WINDOWS_MAX_PATH = 260

    @staticmethod
    def _split_prefix(prefix: PurePosixPath | str) -> Tuple[Optional[str], Optional[str]]:
        """
        Validates & splits the given prefix into a "directory path" and a prefix.
        Throws ValueError if the prefix is invalid, thus this can be used to validate the prefix.

        :param prefix: prefix of objects to list. prefix can end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        :return: a tuple of (directory_path, name_prefix)
        """
        s_prefix = str(prefix)
        if s_prefix == "":
            return "", ""
        m = IBucket.SPLIT_PREFIX_RE.match(s_prefix)
        if m:
            dir_prefix = m.group(1) or ""
            name_prefix = m.group(2)
            assert isinstance(name_prefix, str)
            return dir_prefix, name_prefix
        raise ValueError(f"Invalid S3 prefix: {prefix}")

    @staticmethod
    def _encode_content(content: Union[str, bytes, bytearray]) -> bytes:
        validate(isinstance(content, (str, bytes, bytearray)), f"content must be str, bytes or bytearray, but got {type(content)}")
        return content if isinstance(content, (bytes, bytearray)) else content.encode(IBucket.DEFAULT_ENCODING)

    @staticmethod
    def _validate_name(obj_name: PurePosixPath | str) -> str:
        """
        Validates the given object name.
        Throws ValueError if the object name is invalid, thus this can be used to validate the object name.

        Returns the object name as a string.
        """
        if isinstance(obj_name, PurePosixPath):
            obj_name = str(obj_name)
        validate(IBucket.OBJ_NAME_RE.match(obj_name), f"Invalid S3 object name: {obj_name}")
        return obj_name

    @abstractmethod
    def put_object(self, object_name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_object(self, object_name: PurePosixPath | str) -> bytes:
        """
        :raises FileNotFoundError: if the object is not found
        """
        raise NotImplementedError()

    def fput_object(self, object_name: PurePosixPath | str, file_path: Path) -> None:
        content = file_path.read_bytes()
        self.put_object(object_name, content)

    def fget_object(self, object_name: PurePosixPath | str, file_path: Path) -> None:
        random_suffix = uuid.uuid4().hex[:8]
        tmp_file_path = file_path.parent / f"{file_path.name}.{random_suffix}.part.minio"

        try:
            response = self.get_object(object_name)
            tmp_file_path.write_bytes(response)
            if os.path.exists(file_path):
                os.remove(file_path)  # For windows compatibility.
            os.rename(tmp_file_path, file_path)
        except FileNotFoundError as exc:
            if os.name == "nt":
                if len(str(tmp_file_path)) >= self.WINDOWS_MAX_PATH - self.MINIO_PATH_TEMP_SUFFIX_LEN:
                    raise ValueError(
                        "Reduce the Minio cache path length, Windows has limitation on the path length. "
                        "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation"
                    ) from exc
            raise

        finally:
            if tmp_file_path.exists():
                tmp_file_path.unlink(missing_ok=True)

    def remove_prefix(self, prefix: PurePosixPath | str) -> None:
        """
        Removes all objects with given prefix.
        """
        objects = self.list_objects(prefix)
        self.remove_objects(objects)

    @abstractmethod
    def list_objects(self, prefix: PurePosixPath | str) -> slist[PurePosixPath]:
        """
        Performs a deep/recursive listing of all objects with given prefix.

        :param prefix: prefix of objects to list. prefix can end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        """
        raise NotImplementedError()

    @abstractmethod
    def shallow_list_objects(self, prefix: PurePosixPath | str) -> ShallowListing:
        """
        Performs a shallow listing of all objects with given prefix.
        It will return a list of objects and a list of common prefixes (equivalent to directories on FileSystems).

        :param prefix: prefix of objects to list. prefix can end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, object_name: PurePosixPath | str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def remove_objects(self, list_of_objects: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        """
        This does not return an error when a specified file doesn't exist in the bucket
        It's by design and is consistent with the behavior of similar APIs in Amazon S3.
        This design choice is made for a few reasons: Idempotency, Simplification of Client Logic, Security and Privacy, etc..
        """
        raise NotImplementedError()


class AbstractAppendOnlySynchronizedBucket(IBucket):
    """
    This class is useful for implementing a Bucket having a local FS cache and a remote storage, and the cache is shared between multiple processes,
    so we'll need to synchronize the access to the LocalFS cache.
    """

    def __init__(self, base_bucket: IBucket) -> None:
        self._base_bucket = base_bucket

    def put_object(self, object_name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        self._lock_object(object_name)
        try:
            self._base_bucket.put_object(object_name, content)
        finally:
            self._unlock_object(object_name)

    def get_object(self, object_name: PurePosixPath | str) -> bytes:
        if self.exists(object_name):
            return self._base_bucket.get_object(object_name)
        self._lock_object(object_name)
        try:
            content = self._base_bucket.get_object(object_name)
        finally:
            self._unlock_object(object_name)
        return content

    def list_objects(self, prefix: PurePosixPath | str) -> slist[PurePosixPath]:
        return self._base_bucket.list_objects(prefix)

    def shallow_list_objects(self, prefix: PurePosixPath | str) -> ShallowListing:
        return self._base_bucket.shallow_list_objects(prefix)

    def exists(self, object_name: PurePosixPath | str) -> bool:
        return self._base_bucket.exists(object_name)

    def remove_objects(self, list_of_objects: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        raise io.UnsupportedOperation("remove_objects is not supported for AbstractAppendOnlySynchronizedBucket")

    @abstractmethod
    def _lock_object(self, object_name: PurePosixPath | str):
        raise NotImplementedError()

    @abstractmethod
    def _unlock_object(self, object_name: PurePosixPath | str):
        raise NotImplementedError()
