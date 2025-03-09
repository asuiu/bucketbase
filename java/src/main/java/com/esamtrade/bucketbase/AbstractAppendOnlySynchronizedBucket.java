package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class AbstractAppendOnlySynchronizedBucket extends BaseBucket {
    private final BaseBucket baseBucket;

    public AbstractAppendOnlySynchronizedBucket(BaseBucket baseBucket) {
        this.baseBucket = baseBucket;
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) throws IOException {
        lockObject(name);
        try {
            baseBucket.putObject(name, content);
        } finally {
            unlockObject(name);
        }
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
        lockObject(name);
        try {
            baseBucket.putObjectStream(name, stream);
        } finally {
            unlockObject(name);
        }
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws IOException {
        if (exists(name)) {
            return baseBucket.getObject(name);
        }
        lockObject(name);
        try {
            return baseBucket.getObject(name);
        } finally {
            unlockObject(name);
        }
    }

    @Override
    public ObjectStream getObjectStream(PurePosixPath name) throws IOException {
        if (exists(name)) {
            return baseBucket.getObjectStream(name);
        }
        lockObject(name);
        try {
            return baseBucket.getObjectStream(name);
        } finally {
            unlockObject(name);
        }
    }

    @Override
    public List<PurePosixPath> listObjects(PurePosixPath prefix) throws IOException {
        return baseBucket.listObjects(prefix);
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPath prefix) throws IOException {
        return baseBucket.shallowListObjects(prefix);
    }

    @Override
    public boolean exists(PurePosixPath name) throws IOException {
        return baseBucket.exists(name);
    }

    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) throws IOException {
        throw new UnsupportedOperationException("remove_objects is not supported for AbstractAppendOnlySynchronizedBucket");
    }

    protected abstract void lockObject(PurePosixPath name);

    protected abstract void unlockObject(PurePosixPath name);
}
