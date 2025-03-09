package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PurePosixPathTest {

    @Test
    void nominal_test() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        assertEquals("/home/user", basePath.toString());
        assertEquals(Arrays.asList("", "home", "user"), Arrays.asList(basePath.parts));
        basePath = new PurePosixPath("home", "user");
        assertEquals("home/user", basePath.toString());
        assertEquals(Arrays.asList("home", "user"), Arrays.asList(basePath.parts));
    }

    @Test
    void slash_ending_path() {
        PurePosixPath basePath = new PurePosixPath("/home/user/");
        assertEquals("/home/user/", basePath.toString());
        assertEquals(Arrays.asList("", "home", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("/home/user", "/");
        assertEquals(Arrays.asList("", "home", "user", ""), Arrays.asList(basePath.parts));
        basePath = new PurePosixPath("home/user", "/");
        assertEquals(Arrays.asList("home", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("./home/user", "/.");
        assertEquals(Arrays.asList("home", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath(".", "./home/./user", "/.");
        assertEquals(Arrays.asList("home", "user", ""), Arrays.asList(basePath.parts));
    }

    @Test
    void handling_updir_path() {
        PurePosixPath basePath = new PurePosixPath("/home/user/..");
        assertEquals("/home/", basePath.toString());
        assertEquals(Arrays.asList("", "home", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("/home/../user", "/");
        assertEquals(Arrays.asList("", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("home/../user", "/");
        assertEquals(Arrays.asList("user", ""), Arrays.asList(basePath.parts));
    }

    @Test
    void join_appendsSinglePathCorrectly() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        PurePosixPath resultPath = basePath.join("docs");
        assertEquals("/home/user/docs", resultPath.toString());

        basePath = new PurePosixPath("/home/user/");
        resultPath = basePath.join("./docs/");
        assertEquals("/home/user/docs/", resultPath.toString());
    }

    @Test
    void join_appendsMultiplePathsCorrectly() {
        PurePosixPath basePath = new PurePosixPath("/home");
        PurePosixPath resultPath = basePath.join("user", "docs");
        assertEquals("/home/user/docs", resultPath.toString());

        basePath = new PurePosixPath("/home/");
        resultPath = basePath.join("./user/", "./../docs/./", "./last-path/.");
        assertEquals("/home/docs/last-path/", resultPath.toString());
    }

    @Test
    void parent_returnsParentPathForNonRoot() {
        PurePosixPath path = new PurePosixPath("/home/user/docs");
        assertEquals("/home/user", path.parent().toString());
    }

    @Test
    void parent_returnsRootForRootPath() {
        PurePosixPath path = new PurePosixPath("/");
        // expect to raise exception when call path.parent() on the root
        assertThrows(IllegalArgumentException.class, path::parent);

        path = new PurePosixPath("");
        assertThrows(IllegalArgumentException.class, path::parent);
    }

    @Test
    void name_returnsFileNameForFilePath() {
        PurePosixPath path = new PurePosixPath("/home/user/docs/file.txt");
        assertEquals("file.txt", path.name());
    }

    @Test
    void resolve_resolvesRelativePathAgainstBasePath() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        PurePosixPath resultPath = basePath.resolve("docs");
        assertEquals("/home/user/docs", resultPath.toString());
    }

    @Test
    void resolve_usesOtherPathIfAbsolute() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        PurePosixPath resultPath = basePath.resolve("/etc");
        assertEquals("/etc", resultPath.toString());
    }

    @Test
    void isAbsolute_identifiesAbsolutePath() {
        PurePosixPath path = new PurePosixPath("/home/user");
        assertTrue(path.isAbsolute());
    }

    @Test
    void isAbsolute_identifiesRelativePath() {
        PurePosixPath path = new PurePosixPath("home/user");
        assertFalse(path.isAbsolute());
    }

    @Test
    void isRelativeTo_identifiesSubPath() {
        PurePosixPath basePath = new PurePosixPath("/home");
        PurePosixPath otherPath = new PurePosixPath("/home/user");
        assertTrue(otherPath.isRelativeTo(basePath));
    }

    @Test
    void isRelativeTo_identifiesNonSubPath() {
        PurePosixPath basePath = new PurePosixPath("/home");
        PurePosixPath otherPath = new PurePosixPath("/etc");
        assertFalse(otherPath.isRelativeTo(basePath));
    }

    @Test
    void suffix_returnsFileExtension() {
        PurePosixPath path = new PurePosixPath("/home/user/file.tar.gz");
        assertEquals(".gz", path.suffix());
    }

    @Test
    void suffixes_returnsAllFileExtensions() {
        PurePosixPath path = new PurePosixPath("/home/user/archive.tar.gz");
        assertEquals(Arrays.asList(".tar", ".gz"), path.suffixes());
    }

    @Test
    void stem_returnsFileNameWithoutExtension() {
        PurePosixPath path = new PurePosixPath("/home/user/file.txt");
        assertEquals("file", path.stem());
    }

    @Test
    void get() {
        PurePosixPath path = new PurePosixPath("/home/user/file.txt");
        assertEquals("", path.get(0));
        assertEquals("home", path.get(1));
        assertEquals("user", path.get(2));
        assertEquals("file.txt", path.get(3));
    }

    @Test
    void testEquals() {
        PurePosixPath path1 = new PurePosixPath("/home/./user/file.txt");
        PurePosixPath path2 = new PurePosixPath("/home/user/./file.txt");
        assertEquals(path1, path2);
    }

    @Test
    void testHashCode() {
        PurePosixPath path1 = new PurePosixPath("/home/user/file.txt");
        PurePosixPath path2 = new PurePosixPath("/home/user/file.txt");
        assertEquals(path1.hashCode(), path2.hashCode());
    }

    @Test
    void invalidPath() {
        assertThrows(NullPointerException.class, () -> new PurePosixPath(null));
    }
}