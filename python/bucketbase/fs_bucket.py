import os
import threading
from io import BytesIO
from pathlib import Path, PurePosixPath
from threading import RLock
from time import time_ns, sleep
from typing import Dict, Iterable, Optional, Union, BinaryIO

from streamerate import slist

from bucketbase.errors import DeleteError
from bucketbase.file_lock import FileLockForPath
from bucketbase.ibucket import ShallowListing, IBucket, AbstractAppendOnlySynchronizedBucket, ObjectStream


class FSObjectStream(ObjectStream):
    def __init__(self, path: Path, name: PurePosixPath) -> None:
        self._path = path
        self._name = name
        self._file = None

    def __enter__(self) -> BinaryIO:
        self._file = self._path.open("rb")
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._file.close()
        self._file = None


class FSBucket(IBucket):
    """
    Implements IObjectStorage interface, but stores all objects in local-mounted filesystem.
    """
    BUFFER_SIZE = 64 * 1024

    def __init__(self, root: Path) -> None:
        assert isinstance(root, Path), f"root must be a Path, but got {type(root)}"
        if not root.exists():
            root.mkdir(parents=True, exist_ok=True)
        assert root.is_dir(), f"root must be a directory, but got {root}"
        self._root = root

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        stream = BytesIO(content) if isinstance(content, (bytes, bytearray)) else BytesIO(content.encode())
        self.put_object_stream(name, stream)

    def _temp_object_absolute_path(self, name: PurePosixPath | str) -> Path:
        tmp_obj_name = str(PurePosixPath(name)).replace("/","#")
        return self._root / self.BUCKETBASE_TMP_DIR_NAME / f"{tmp_obj_name}@{str(int(time_ns()))}-{threading.get_ident()}.tmp"

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        _name = self._validate_name(name)
        _object_path = self._root / _name

        _object_path_temp = self._temp_object_absolute_path(name)
        try:
            _object_path_temp.parent.mkdir(parents=True, exist_ok=True)
            with _object_path_temp.open("wb") as f:
                while chunk := stream.read(self.BUFFER_SIZE):
                    f.write(chunk)
            _object_path.parent.mkdir(parents=True, exist_ok=True)
            for attempt in range(10):
                try:
                    os.replace(_object_path_temp, _object_path)
                    return
                except PermissionError:
                    if attempt < 9:
                        sleep(0.05)
                    else:
                        raise
        except FileNotFoundError as exc:
            if os.name == "nt":
                if len(str(_object_path)) >= self.WINDOWS_MAX_PATH - self.MINIO_PATH_TEMP_SUFFIX_LEN:
                    raise ValueError(
                        "Reduce the Minio cache path length, Windows has limitation on the path length. "
                        "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation"
                    ) from exc
            raise

    def get_object(self, name: PurePosixPath | str) -> bytes:
        """
        :raises FileNotFoundError: if the object is not found
        """
        _name = self._validate_name(name)
        _path = self._root / _name
        return _path.read_bytes()

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        _name = self._validate_name(name)
        fpath = self._root / _name
        if not fpath.exists() or not fpath.is_file():
            raise FileNotFoundError(f"Object {_name} not found in FSBucket")
        return FSObjectStream(fpath, PurePosixPath(name))

    def _get_recurs_listing(self, root: Path, s_prefix: str) -> slist[PurePosixPath]:
        listing = root.rglob("*")
        matching_objects = slist()
        for path in listing:
            # get the last part of the path relative to the self._root
            relative_path = path.relative_to(self._root)
            if relative_path.as_posix().startswith(s_prefix) and path.is_file():
                matching_objects.append(PurePosixPath(relative_path))
        return matching_objects

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        """
        Performs a deep/recursive listing of all objects with given prefix.
        """
        do_exclude_tmp_dir = False
        if IBucket.BUCKETBASE_TMP_DIR_NAME.startswith(str(prefix)):
            if prefix.startswith(IBucket.BUCKETBASE_TMP_DIR_NAME):
                raise ValueError(f"Prefix {prefix} is not allowed as it starts with the reserved name {IBucket.BUCKETBASE_TMP_DIR_NAME}")
            do_exclude_tmp_dir = True

        dir_path, _ = self._split_prefix(prefix)
        s_prefix = str(prefix)

        start_list_lpath = self._root / dir_path

        # Here we do an optimization to avoid listing all files in the root of the ObjectStorage
        matching_objects = self._get_recurs_listing(start_list_lpath, s_prefix)

        if do_exclude_tmp_dir:
            matching_objects = matching_objects.filter(lambda x: not x.as_posix().startswith(IBucket.BUCKETBASE_TMP_DIR_NAME))

        return matching_objects

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        """
        if IBucket.BUCKETBASE_TMP_DIR_NAME.startswith(str(prefix)):
            if prefix.startswith(IBucket.BUCKETBASE_TMP_DIR_NAME):
                raise ValueError(f"Prefix {prefix} is not allowed as it starts with the reserved name {IBucket.BUCKETBASE_TMP_DIR_NAME}")

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
                if dir_path.startswith(IBucket.BUCKETBASE_TMP_DIR_NAME):
                    continue
                prefixes.append(dir_path)
            else:
                raise ValueError(f"Unexpected path type: {p}")
        return ShallowListing(objects=matching_objects, prefixes=prefixes)

    def exists(self, name: PurePosixPath | str) -> bool:
        _name = self._validate_name(name)
        _obj_path = self._root / _name
        return _obj_path.exists() and _obj_path.is_file()

    def _try_remove_empty_dirs(self, p):
        dir_to_remove = p.parent
        while dir_to_remove.relative_to(self._root).parts:
            try:
                dir_to_remove.rmdir()
            except OSError:
                break
            dir_to_remove = dir_to_remove.parent

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        """
        Note: Please bear in mind that this is not concurrent safe.
        Attention!!! The removal of objects is not atomic due to sequential removal of leftover directories.

        There's a way to make a sync version using FileLockForPath, but it will penalize the performance.
        """
        delete_errors = slist()
        for obj in names:
            obj = self._validate_name(obj)
            p = self._root / obj
            try:
                p.unlink(missing_ok=True)
            except Exception as e:
                delete_errors.append(DeleteError(code=404, message=e, name=str(obj), version_id=None))
            else:
                self._try_remove_empty_dirs(p)
        return delete_errors

    def get_root(self) -> Path:
        """ This is not part of the IBucket interface, but it's useful for multiple purposes. """
        return self._root

    def get_size(self, name: PurePosixPath | str) -> int:
        return os.stat(self._root / name).st_size


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

    def _lock_object(self, name: PurePosixPath | str):
        name = self._validate_name(name)
        lock_object_name = name.replace(self.SEP, "$")
        with self._my_lock:
            if lock_object_name in self._locks:
                file_lock = self._locks[lock_object_name]
            else:
                file_lock = FileLockForPath(self._locks_path / lock_object_name)
                self._locks[lock_object_name] = file_lock
        file_lock.acquire()

    def _unlock_object(self, name: PurePosixPath | str):
        name = self._validate_name(name)
        lock_object_name = name.replace(self.SEP, "$")
        with self._my_lock:
            if lock_object_name not in self._locks:
                raise RuntimeError(f"Object {name} is not locked")
            file_lock = self._locks[lock_object_name]
            file_lock.release()

    @classmethod
    def build(cls, root: Path, locks_path: Optional[Path] = None) -> "AppendOnlyFSBucket":
        if locks_path is None:
            locks_path = root / cls.BUCKETBASE_TMP_DIR_NAME / "__locks__"
        return cls(FSBucket(root), locks_path)
