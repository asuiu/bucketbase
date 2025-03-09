package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class ObjectStream implements AutoCloseable {
    private final InputStream stream;
    private final String name;

    public ObjectStream(InputStream stream, String name) {
        this.stream = stream;
        this.name = name;
    }

    public InputStream getStream() {
        return stream;
    }

    public String getName() {
        return name;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
