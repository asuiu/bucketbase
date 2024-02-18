# BucketBase

BucketBase offers a comprehensive suite of base classes tailored for abstracting object storage solutions. It's designed to facilitate seamless integration with different storage backends, including S3-compatible services (like Minio), in-memory storage for testing, and file-based storage for local development and caching purposes. This flexibility makes it an ideal choice for projects that require a unified API for object storage across various environments.

## Features

- **S3 Compatible Storage Support:** Use with any S3-compatible storage service like Minio, AWS S3, etc.
- **In-memory Storage:** Perfect for testing purposes, avoiding the need for an actual object storage service.
- **File Storage:** Ideal for local development and caching, simulating an object storage interface with local file system.
- **Extensible:** Implement the `IBucket` interface to support additional storage backends.

## Installation

Install BucketBase using pip:

```bash
pip install bucketbase
```
Quick Start
To get started with BucketBase, first import the storage class you wish to use:

```python
from bucketbase.memory_bucket import MemoryBucket
from bucketbase.minio_bucket import MinioBucket
from bucketbase.fs_bucket import FSBucket, AppendOnlyFSBucket
```

### Using Memory Storage
Memory storage is useful for tests or development environments where no persistence is required.

```python
bucket = MemoryBucket()
bucket.put_object("test.txt", "Hello, World!")
print(bucket.get_object("test.txt"))  # Outputs: b'Hello, World!'
```

### Using File Storage
File storage is useful for local development and caching.

```python
bucket = FSBucket("/path/to/storage")
bucket.put_object("hello.txt", "Hello, File Storage!")
print(bucket.get_object("hello.txt"))  # Outputs: b'Hello, File Storage!'
```

### Using Minio (S3 Compatible Storage)
Ensure you have Minio server running and accessible.

```python
from bucketbase.minio_bucket import MinioBucket

bucket = MinioBucket(endpoint="localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
bucket.put_object("greet.txt", "Hello, Minio!")
print(bucket.get_object("greet.txt"))  # Outputs: b'Hello, Minio!'
```

## Advanced Usage
For advanced usage, including error handling, listing objects, and more, refer to the IBucket interface and the specific implementation you are using. Each storage option may have additional methods and features tailored to its backend.

## Contributing
Contributions are welcome! If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are warmly welcome.

## Licensing
The code in this project is licensed under MIT license.

### Changelog

##### 1.2.0 (breaking changes)
- rename all IBucket method args: object_name -> name
- rename args: list_of_objects -> names

##### 1.1.0 (breaking changes)
- IBucket rename: get_object_content() -> get_object()
- IBucket.fput_oject() rename arg: destination -> file_path

##### 1.0.1
- Added ShallowListing to __init__.py

---
This README.md provides a general overview of BucketBase, instructions for installation, a quick start guide for using its various storage options, and an invitation for contributions. It's structured to be informative and straightforward, allowing users to quickly get started with the project.
