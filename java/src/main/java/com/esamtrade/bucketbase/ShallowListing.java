package com.esamtrade.bucketbase;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class ShallowListing {
    private final List<PurePosixPath> objects;
    private final List<PurePosixPath> prefixes;

    public ShallowListing(List<PurePosixPath> objects, List<PurePosixPath> prefixes) {
        this.objects = Collections.unmodifiableList(objects);
        this.prefixes = Collections.unmodifiableList(prefixes);
    }

    public List<PurePosixPath> getObjects() {
        return objects;
    }

    public List<PurePosixPath> getPrefixes() {
        return prefixes;
    }
}
