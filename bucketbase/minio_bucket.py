import io
import logging
import os
import traceback
from pathlib import Path, PurePosixPath
from typing import Iterable, Union

import certifi
import minio
import urllib3
from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from multiminio import MultiMinio
from streamerate import slist, stream

from bucketbase.ibucket import ShallowListing, IBucket


def build_minio_client(endpoints: str,
                       access_key: str,
                       secret_key: str,
                       secure: bool = True,
                       region: str | None = "custom",
                       conn_pool_size: int = 128,
                       timeout: int = 5) -> Minio:
    """
    :param endpoints: comma separated list of endpoints
    :param access_key: access key
    :param secret_key: secret key
    :param secure: use SSL
    :param region: region
    :param conn_pool_size: connection pool size
    :param timeout: timeout in seconds
    """
    ca_certs = os.environ.get("SSL_CERT_FILE") or certifi.where()
    https_pool_manager = urllib3.PoolManager(
        timeout=timeout,
        maxsize=conn_pool_size,
        cert_reqs="CERT_REQUIRED",
        ca_certs=ca_certs,
        retries=urllib3.Retry(total=1, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
    )
    # and a non-SSL http client
    http_pool_manager = urllib3.PoolManager(
        timeout=timeout,
        maxsize=conn_pool_size,
        retries=urllib3.Retry(total=1, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
    )

    endpoints = endpoints.split(",")
    minio_clients = []
    for endpoint in endpoints:
        http_client = https_pool_manager if secure else http_pool_manager
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
            http_client=http_client,
        )
        minio_clients.append(minio_client)
    if len(minio_clients) > 1:
        multi_minio_client = MultiMinio(clients=minio_clients, max_try_timeout=timeout)
        return multi_minio_client
    return minio_clients[0]


class MinioBucket(IBucket):
    def __init__(self, bucket_name: str, minio_client: Minio) -> None:
        self._minio_client = minio_client
        self._bucket_name = bucket_name

    def get_object_content(self, object_name: PurePosixPath | str) -> bytes:
        _object_name = self._validate_name(object_name)
        try:
            response = self._minio_client.get_object(self._bucket_name, _object_name)
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"Object {_object_name} not found in bucket {self._bucket_name} on Minio") from e
            raise
        try:
            data = bytes()
            for buffer in response.stream(amt=1024 * 1024):
                data += buffer
        finally:
            response.release_conn()
        return data

    def fget_object(self, object_name: PurePosixPath | str, file_path: Path) -> None:
        _object_name = self._validate_name(object_name)
        self._minio_client.fget_object(self._bucket_name, _object_name, str(file_path))

    def put_object(self, object_name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        _content = self._encode_content(content)
        _object_name = self._validate_name(object_name)
        f = io.BytesIO(_content)
        self._minio_client.put_object(bucket_name=self._bucket_name, object_name=_object_name, data=f, length=len(_content))

    def fput_object(self, object_name: PurePosixPath | str, destination: Path) -> None:
        _object_name = self._validate_name(object_name)
        self._minio_client.fput_object(self._bucket_name, _object_name, str(destination))

    def list_objects(self, prefix: PurePosixPath | str) -> slist[PurePosixPath]:
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=True)
        object_names = stream(listing_itr).map(Object.object_name.fget).map(PurePosixPath).to_list()
        return object_names

    def shallow_list_objects(self, prefix: PurePosixPath | str) -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        """
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=False)
        object_names = stream(listing_itr).map(Object.object_name.fget).to_list()
        prefixes = object_names.filter(lambda x: x.endswith("/")).to_list()
        objects = object_names.filter(lambda x: not x.endswith("/")).map(PurePosixPath).to_list()
        return ShallowListing(objects=objects, prefixes=prefixes)

    def exists(self, object_name: PurePosixPath | str) -> bool:
        _object_name = self._validate_name(object_name)
        try:
            self._minio_client.stat_object(self._bucket_name, _object_name)
            return True
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                return False
            logging.exception(traceback.print_exc())
            raise

    def remove_objects(self, list_of_objects: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        delete_objects_stream = stream(list_of_objects).map(self._validate_name).map(DeleteObject)

        # the return value is a generator and if will not be converted to a list the deletion won't happen
        errors = slist(self._minio_client.remove_objects(self._bucket_name, delete_objects_stream))
        return errors
