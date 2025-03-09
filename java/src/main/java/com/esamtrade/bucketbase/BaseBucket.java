package com.esamtrade.bucketbase;


import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



class S3Utils {
    public static final String S3_NAME_CHARS_NO_SEP = "\\w!\\-\\.\\)\\(";
    public static final Pattern S3_NAME_SAFE_RE = Pattern.compile("^[{S3_NAME_CHARS_NO_SEP}][{S3_NAME_CHARS_NO_SEP}/]+$");
}

public abstract class BaseBucket implements IBucket {
    protected static final String SEP = "/";
    protected static final Pattern SPLIT_PREFIX_RE = Pattern.compile("^((?:[" + S3Utils.S3_NAME_CHARS_NO_SEP + "]+/)*)([" + S3Utils.S3_NAME_CHARS_NO_SEP + "]*)$");
    protected static final Pattern OBJ_NAME_RE = Pattern.compile("^(?:[" + S3Utils.S3_NAME_CHARS_NO_SEP + "]+/)*[" + S3Utils.S3_NAME_CHARS_NO_SEP + "]+$");
    protected static final String DEFAULT_ENCODING = "utf-8";
    protected static final int MINIO_PATH_TEMP_SUFFIX_LEN = 43;
    protected static final int WINDOWS_MAX_PATH = 260;

    public static class Tuple<T, U> {
        public final T first;
        public final U second;

        public Tuple(T first, U second) {
            this.first = first;
            this.second = second;
        }
    }

    protected static Tuple<String, String> splitPrefix(PurePosixPath prefix) {
        String prefixStr = prefix.toString();
        if (prefixStr.isEmpty()) {
            return new Tuple<>("", "");
        }
        Matcher matcher = SPLIT_PREFIX_RE.matcher(prefixStr);
        if (matcher.matches()) {
            String dirPrefix = Optional.ofNullable(matcher.group(1)).orElse("");
            String namePrefix = Optional.ofNullable(matcher.group(2)).orElse("");
            return new Tuple<>(dirPrefix, namePrefix);
        }
        throw new IllegalArgumentException("Invalid S3 prefix: " + prefixStr);
    }

    protected static byte[] encodeContent(String content) throws UnsupportedEncodingException {
        return content.getBytes(DEFAULT_ENCODING);
    }

    protected static byte[] encodeContent(byte[] content) {
        return content;
    }

    protected static String validateName(String name) {
        if (!OBJ_NAME_RE.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid S3 object name: " + name);
        }
        return name;
    }
    protected static String validateName(PurePosixPath name) {
        String nameStr = name.toString();
        return validateName(nameStr);
    }

    public abstract void putObject(PurePosixPath name, byte[] content) throws IOException;

    public abstract void putObjectStream(PurePosixPath name, InputStream stream) throws IOException;

    public abstract byte[] getObject(PurePosixPath name) throws IOException;

    public abstract ObjectStream getObjectStream(PurePosixPath name) throws IOException;

    public void fputObject(PurePosixPath name, Path filePath) throws IOException {
        byte[] content = Files.readAllBytes(filePath);
        putObject(name, content);
    }

    public void fgetObject(PurePosixPath name, Path filePath) throws IOException {
        String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
        Path tmpFilePath = filePath.getParent().resolve("_" + filePath.getFileName() + "." + randomSuffix + ".part");

        try {
            byte[] response = getObject(name);
            Files.write(tmpFilePath, response);
            Files.move(tmpFilePath, filePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exc) {
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                if (tmpFilePath.toString().length() >= WINDOWS_MAX_PATH - MINIO_PATH_TEMP_SUFFIX_LEN) {
                    throw new IllegalArgumentException(
                            "Reduce the Minio cache path length, Windows has limitation on the path length. " +
                                    "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation",
                            exc
                    );
                }
            }
            throw exc;
        } finally {
            Files.deleteIfExists(tmpFilePath);
        }
    }

    public void removePrefix(PurePosixPath prefix) throws IOException {
        List<PurePosixPath> objects = listObjects(prefix);
        removeObjects(objects);
    }

    public abstract List<PurePosixPath> listObjects(PurePosixPath prefix) throws IOException;

    public abstract ShallowListing shallowListObjects(PurePosixPath prefix) throws IOException;

    public abstract boolean exists(PurePosixPath name) throws IOException;

    public abstract List<DeleteError>  removeObjects(List<PurePosixPath> names) throws IOException;

    public void copyPrefix(BaseBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) throws IOException {
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be greater than 0");
        }

        List<PurePosixPath> srcObjects = listObjects(srcPrefix);
        String srcPrefixStr = srcPrefix.toString();
        String dstPrefixStr = dstPrefix.toString();
        int srcPrefixLen = srcPrefixStr.length();

        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(threads, srcObjects.size()));
        List<Future<Void>> futures = new ArrayList<>();

        for (PurePosixPath srcObj : srcObjects) {
            futures.add(executorService.submit(() -> {
                String objStr = srcObj.toString();
                if (!objStr.startsWith(srcPrefixStr)) {
                    return null;
                }

                String name = dstPrefixStr + objStr.substring(srcPrefixLen);
                if (name.startsWith("/")) {
                    name = name.substring(1);
                }
                dstBucket.putObject(PurePosixPath.from(name), getObject(srcObj));
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new IOException(e);
            }
        }

        executorService.shutdown();
    }

    public void movePrefix(BaseBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) throws IOException {
        copyPrefix(dstBucket, srcPrefix, dstPrefix, threads);
        removePrefix(srcPrefix);
    }
}
