from unittest.case import TestCase

from bucketbase.ibucket import IBucket
from tests.bucket_tester import IBucketTester


class TestIBucket(TestCase):
    def test_split_prefix(self):
        res = IBucket._split_prefix("dir1/dir2/file.txt")
        self.assertEqual(res, ("dir1/dir2/", "file.txt"))

        res = IBucket._split_prefix("dir1/dir2/")
        self.assertEqual(res, ("dir1/dir2/", ""))

        res = IBucket._split_prefix("dir1/dir2")
        self.assertEqual(res, ("dir1/", "dir2"))

        res = IBucket._split_prefix("dir1/")
        self.assertEqual(res, ("dir1/", ""))

        res = IBucket._split_prefix("dir1")
        self.assertEqual(res, ("", "dir1"))

        # Valid characters
        res = IBucket._split_prefix("!'(file)'-1_2.txt")
        self.assertEqual(res, ("", "!'(file)'-1_2.txt"))

        # Invalid Prefix cases
        for prefix in IBucketTester.INVALID_PREFIXES:
            self.assertRaises(ValueError, IBucket._split_prefix, prefix)
