from pathlib import PurePosixPath
from unittest import TestCase

from streamerate import slist
from tsx import iTSms

from bucketbase.ibucket import ShallowListing, IBucket

if __name__ == '__main__':
    pass


class IBucketTester:
    INVALID_PREFIXES = ["/", "/dir", "dir1//dir2", "dir1//", "star*1", "dir1/a\file.txt", "at@gmail", "sharp#1", "dollar$1", "comma,"]

    def __init__(self, storage: IBucket, test_case: TestCase) -> None:
        self.storage = storage
        self.test_case = test_case
        # Next is a unique suffix to be used in the names of dirs and files, so they will be unique
        self.us = f"{iTSms.now() % 100_000_000:08d}"

    def cleanup(self):
        self.storage.remove_prefix(f"dir{self.us}")

    def test_put_and_get_object(self):
        unique_dir = f"dir{self.us}"
        # binary content
        path = PurePosixPath(f"{unique_dir}/file1.bin")
        b_content = b"Test content"
        self.storage.put_object(path, b_content)
        retrieved_content = self.storage.get_object_content(path)
        self.test_case.assertEqual(retrieved_content, b_content)

        # str content
        path = PurePosixPath(f"{unique_dir}/file1.txt")
        s_content = "Test content"
        self.storage.put_object(path, s_content)
        retrieved_content = self.storage.get_object_content(path)
        self.test_case.assertEqual(retrieved_content, bytes(s_content, "utf-8"))

        # bytearray content
        path = PurePosixPath(f"{unique_dir}/file1.ba")
        ba_content = bytearray(b"Test content")
        self.storage.put_object(path, ba_content)
        retrieved_content = self.storage.get_object_content(path)
        self.test_case.assertEqual(retrieved_content, b_content)

        # string path
        path = f"{unique_dir}/file1.txt"
        s_content = "Test content"
        self.storage.put_object(path, s_content)
        retrieved_content = self.storage.get_object_content(path)
        self.test_case.assertEqual(retrieved_content, bytes(s_content, "utf-8"))

        # inexistent path
        path = f"{unique_dir}/inexistent.txt"
        self.test_case.assertRaises(FileNotFoundError, self.storage.get_object_content, path)

    def test_list_objects(self):
        unique_dir = f"dir{self.us}"
        self.storage.put_object(PurePosixPath(f"{unique_dir}/file1.txt"), b"Content 1")
        self.storage.put_object(PurePosixPath(f"{unique_dir}/dir2/file2.txt"), b"Content 2")
        self.storage.put_object(PurePosixPath(f"{unique_dir}file1.txt"), b"Content 3")
        objects = self.storage.list_objects(PurePosixPath(f"{unique_dir}"))
        objects.sort()
        self.test_case.assertIsInstance(objects, slist)
        expected_objects = [PurePosixPath(f"{unique_dir}/dir2/file2.txt"), PurePosixPath(f"{unique_dir}/file1.txt"), PurePosixPath(f"{unique_dir}file1.txt")]
        self.test_case.assertListEqual(objects, expected_objects)

        objects = self.storage.list_objects(f"{unique_dir}/")
        objects.sort()
        self.test_case.assertIsInstance(objects, slist)
        expected_objects = [PurePosixPath(f"{unique_dir}/dir2/file2.txt"), PurePosixPath(f"{unique_dir}/file1.txt")]
        self.test_case.assertListEqual(objects, expected_objects)

        # Invalid Prefix cases
        for prefix in self.INVALID_PREFIXES:
            self.test_case.assertRaises(ValueError, self.storage.list_objects, prefix)

    def test_shallow_list_objects(self):
        unique_dir = f"dir{self.us}"
        self.storage.put_object(PurePosixPath(f"{unique_dir}/file1.txt"), b"Content 1")
        self.storage.put_object(PurePosixPath(f"{unique_dir}/dir2/file2.txt"), b"Content 2")
        self.storage.put_object(PurePosixPath(f"{unique_dir}file1.txt"), b"Content 3")

        self.test_case.assertRaises(ValueError, self.storage.shallow_list_objects, "/")
        self.test_case.assertRaises(ValueError, self.storage.shallow_list_objects, "/d")

        objects = self.storage.shallow_list_objects(f"{unique_dir}/")
        expected_objects = [PurePosixPath(f"{unique_dir}/file1.txt")]
        expected_prefixes = [f"{unique_dir}/dir2/"]
        self.test_case.assertListEqual(objects.objects, expected_objects)
        self.test_case.assertEqual(objects.prefixes, expected_prefixes)

        shallow_listing = self.storage.shallow_list_objects(PurePosixPath(f"{unique_dir}"))
        expected_objects = [PurePosixPath(f"{unique_dir}file1.txt")]
        expected_prefixes = [f"{unique_dir}/"]
        self.test_case.assertIsInstance(shallow_listing.objects, slist)
        self.test_case.assertIsInstance(shallow_listing.prefixes, slist)
        self.test_case.assertListEqual(shallow_listing.objects, expected_objects)
        self.test_case.assertEqual(shallow_listing.prefixes, expected_prefixes)

        # Invalid Prefix cases
        for prefix in self.INVALID_PREFIXES:
            self.test_case.assertRaises(ValueError, self.storage.shallow_list_objects, prefix)

    def test_exists(self):
        unique_dir = f"dir{self.us}"
        path = PurePosixPath(f"{unique_dir}/file.txt")
        self.storage.put_object(path, b"Content")
        self.test_case.assertTrue(self.storage.exists(path))
        self.test_case.assertFalse(self.storage.exists(f"{unique_dir}"))
        self.test_case.assertRaises(ValueError, self.storage.exists, f"{unique_dir}/")

    def test_remove_objects(self):
        # Setup the test
        unique_dir = f"dir{self.us}"
        path1 = PurePosixPath(f"{unique_dir}/file1.txt")
        path2 = PurePosixPath(f"{unique_dir}/file2.txt")
        self.storage.put_object(path1, b"Content 1")
        self.storage.put_object(path2, b"Content 2")

        # perform removal action
        result = self.storage.remove_objects([path1, path2, f"{unique_dir}/inexistent.file"])

        # check that the files do not exist
        self.test_case.assertIsInstance(result, slist)
        self.test_case.assertEqual(result, [])
        self.test_case.assertFalse(self.storage.exists(path1))
        self.test_case.assertFalse(self.storage.exists(path2))
        self.test_case.assertRaises(FileNotFoundError, self.storage.get_object_content, f"{unique_dir}/file1.txt")
        self.test_case.assertRaises(ValueError, self.storage.remove_objects, [f"{unique_dir}/"])

        # check that the leftover empty directories are also removed
        shallow_listing = self.storage.shallow_list_objects("")
        self.test_case.assertEqual(shallow_listing, ShallowListing(objects=slist(), prefixes=slist()))
