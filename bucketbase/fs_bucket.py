import os
from pathlib import Path, PurePosixPath
from threading import RLock
from typing import Dict, Iterable, Optional, Union

from streamerate import slist

from bucketbase.errors import DeleteError
from bucketbase.file_lock import FileLockForPath
from bucketbase.ibucket import ShallowListing, IBucket, AbstractAppendOnlySynchronizedBucket


class FSBucket(IBucket):
    """
    Implements IObjectStorage interface, but stores all objects in local-mounted filesystem.
    """

    def __init__(self, root: Path) -> None:
        assert isinstance(root, Path), f"root must be a Path, but got {type(root)}"
        if not root.exists():
            root.mkdir(parents=True, exist_ok=True)
        assert root.is_dir(), f"root must be a directory, but got {root}"
        self._root = root

    def put_object(self, object_name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        _object_name = self._validate_name(object_name)
        _content = content if isinstance(content, (bytes, bytearray)) else content.encode()
        _object_path = self._root / _object_name
        try:
            _object_path.parent.mkdir(parents=True, exist_ok=True)
            _object_path.write_bytes(_content)
        except FileNotFoundError as exc:
            if os.name == "nt":
                if len(str(_object_path)) >= self.WINDOWS_MAX_PATH - self.MINIO_PATH_TEMP_SUFFIX_LEN:
                    raise ValueError(
                        "Reduce the Minio cache path length, Windows has limitation on the path length. "
                        "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation"
                    ) from exc
            raise

    def get_object_content(self, object_name: PurePosixPath | str) -> bytes:
        """
        :raises FileNotFoundError: if the object is not found
        """
        _object_name = self._validate_name(object_name)
        _path = self._root / _object_name
        return _path.read_bytes()

    def _get_recurs_listing(self, root: Path, s_prefix: str) -> slist[PurePosixPath]:
        listing = root.rglob("*")
        matching_objects = slist()
        for path in listing:
            # get the last part of the path relative to the self._root
            relative_path = path.relative_to(self._root)
            if relative_path.as_posix().startswith(s_prefix) and path.is_file():
                matching_objects.append(PurePosixPath(relative_path))
        return matching_objects

    def list_objects(self, prefix: PurePosixPath | str) -> slist[PurePosixPath]:
        """
        Performs a deep/recursive listing of all objects with given prefix.
        """
        dir_path, _ = self._split_prefix(prefix)
        s_prefix = str(prefix)

        start_list_lpath = self._root / dir_path

        # Here we do an optimization to avoid listing all files in the root of the ObjectStorage
        matching_objects = self._get_recurs_listing(start_list_lpath, s_prefix)
        return matching_objects

    def shallow_list_objects(self, prefix: PurePosixPath | str) -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        """
        dir_path, name_prefix = self._split_prefix(prefix)
        start_list_lpath = self._root / dir_path

        listing = start_list_lpath.glob(name_prefix + "*")
        matching_objects = slist()
        prefixes = slist()
        for p in listing:
            if p.is_file():
                obj_path = PurePosixPath(p.relative_to(self._root))
                matching_objects.append(obj_path)
            elif p.is_dir():
                dir_path = p.relative_to(self._root).as_posix() + "/"
                prefixes.append(dir_path)
            else:
                raise ValueError(f"Unexpected path type: {p}")
        return ShallowListing(objects=matching_objects, prefixes=prefixes)

    def exists(self, object_name: PurePosixPath | str) -> bool:
        _object_name = self._validate_name(object_name)
        _obj_path = self._root / _object_name
        return _obj_path.exists() and _obj_path.is_file()

    def _try_remove_empty_dirs(self, p):
        dir_to_remove = p.parent
        while dir_to_remove.relative_to(self._root).parts:
            try:
                dir_to_remove.rmdir()
            except OSError:
                break
            dir_to_remove = dir_to_remove.parent

    def remove_objects(self, list_of_objects: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        """
        Note: Please bear in mind that this is not concurrent safe.
        Attention!!! The removal of objects is not atomic due to sequential removal of leftover directories.

        There's a way to make a sync version using FileLockForPath, but it will penalize the performance.
        """
        delete_errors = slist()
        for obj in list_of_objects:
            obj = self._validate_name(obj)
            p = self._root / obj
            try:
                p.unlink(missing_ok=True)
            except Exception as e:
                delete_errors.append(DeleteError(code=404, message=e, name=str(obj), version_id=None))
            else:
                self._try_remove_empty_dirs(p)
        return delete_errors


class AppendOnlyFSBucket(AbstractAppendOnlySynchronizedBucket):
    """
    Intended to be used as a local FS cache(for remote bucket), shared between multiple processes, and the cache is append-only, synchronized with file locks.
    """

    def __init__(self, base: IBucket, locks_path: Path) -> None:
        """
        The locks_path should be a local file system path with write permissions.
        """
        super().__init__(base)
        self._locks: Dict[str, FileLockForPath] = {}
        self._my_lock = RLock()
        self._locks_path = locks_path

    def _lock_object(self, object_name: PurePosixPath | str):
        object_name = self._validate_name(object_name)
        lock_object_name = object_name.replace(self.SEP, "$")
        with self._my_lock:
            if lock_object_name in self._locks:
                file_lock = self._locks[lock_object_name]
            else:
                file_lock = FileLockForPath(self._locks_path / lock_object_name)
                self._locks[lock_object_name] = file_lock
        file_lock.acquire()

    def _unlock_object(self, object_name: PurePosixPath | str):
        object_name = self._validate_name(object_name)
        lock_object_name = object_name.replace(self.SEP, "$")
        with self._my_lock:
            if lock_object_name not in self._locks:
                raise RuntimeError(f"Object {object_name} is not locked")
            file_lock = self._locks[lock_object_name]
            file_lock.release()

    @classmethod
    def build(cls, root: Path, locks_path: Optional[Path] = None) -> "AppendOnlyFSBucket":
        if locks_path is None:
            locks_path = root / "__locks__"
        return cls(FSBucket(root), locks_path)
