package com.esamtrade.bucketbase;

import org.apache.commons.io.FilenameUtils;

import java.nio.file.Path;
import java.util.*;

public class PurePosixPath implements Comparable<PurePosixPath> {

    protected final String[] parts;
    public final String SEP = "/";

    public static PurePosixPath from(String path, String... more) {
        return new PurePosixPath(path, more);
    }

    public static PurePosixPath from(Path path) {
        String name = path.toString();
        String posixPath = FilenameUtils.separatorsToUnix(name);
        return new PurePosixPath(posixPath);
    }

    public PurePosixPath(String first, String... more) {
        List<String> allParts = new ArrayList<>();
        if (!first.isEmpty()) {
            allParts.addAll(Arrays.asList(first.split("/", -1)));
        }
        for (String part : more) {
            if (!part.isEmpty()) {
                allParts.addAll(Arrays.asList(part.split("/", -1)));
            }
        }
        this.parts = normalizeParts(allParts);
    }

    public PurePosixPath(String[] paths) {
        this.parts = normalizeParts(Arrays.asList(paths));
    }

    private String[] normalizeParts(List<String> parts) {
        List<String> result = new ArrayList<>();
        int i = 0;
        int n = parts.size();
        // add the first part if it is empty
        if (!parts.isEmpty() && parts.get(0).isEmpty()) {
            result.add("");
            i = 1;
        }
        while (i < n - 1) {
            String part = parts.get(i++);
            if (part.equals("..")) {
                if (!result.isEmpty() && !result.get(result.size() - 1).equals("..")) {
                    result.remove(result.size() - 1);
                } else {
                    result.add(part);
                }
            } else if (!part.equals(".") && !part.isEmpty()) {
                result.add(part);
            }
        }
        if (i < n) {
            String part = parts.get(i);
            if (part.equals("..")) {
                if (!result.isEmpty() && !result.get(result.size() - 1).equals("..")) {
                    result.set(result.size() - 1, ""); // set the last part to empty, as the .. means reference to directory
                } else {
                    result.add(part);
                }
            } else if (part.equals(".")) {
                result.add("");
            } else {
                result.add(part);
            }
        }
        return result.toArray(new String[0]);
    }


    public PurePosixPath join(String other, String... more) {
        String[] combined = new String[1 + more.length];
        combined[0] = other;
        System.arraycopy(more, 0, combined, 1, more.length);
        return new PurePosixPath(this.toString(), combined);
    }

    public PurePosixPath join(PurePosixPath other) {
        return new PurePosixPath(this.toString(), other.toString());
    }

    public PurePosixPath parent() {
        int i = parts.length - 1;
        if (i < 0) {
            throw new IllegalArgumentException("Path " + this + " has no parent");
        }
        if (parts[i].isEmpty())
            --i;
        if (parts[i].isEmpty())
            throw new IllegalArgumentException("Path " + this + " has no parent");
        String[] parentParts = Arrays.copyOf(parts, i);
        return new PurePosixPath(parentParts);
    }

    public String name() {
        if (parts.length == 0) {
            return "";
        }
        return parts[parts.length - 1];
    }


    public PurePosixPath resolve(String other) {
        if (other.startsWith("/")) {
            return new PurePosixPath(other);
        }
        return new PurePosixPath(this.toString(), other);
    }

    public PurePosixPath resolve(PurePosixPath other) {
        return this.resolve(other.toString());
    }


    public List<String> parts() {
        return Collections.unmodifiableList(Arrays.asList(parts));
    }

    public String get(int index) {
        return parts[index];
    }

    public String suffix() {
        String name = name();
        int index = name.lastIndexOf(".");
        if (index == -1) {
            return "";
        }
        return name.substring(index);
    }

    public List<String> suffixes() {
        String name = name();
        List<String> suffixes = new ArrayList<>();
        int index = name.lastIndexOf(".");
        while (index != -1) {
            suffixes.add(name.substring(index));
            name = name.substring(0, index);
            index = name.lastIndexOf(".");
        }
        Collections.reverse(suffixes);
        return suffixes;
    }

    public String stem() {
        String name = name();
        int index = name.lastIndexOf(".");
        if (index == -1) {
            return name;
        }
        return name.substring(0, index);
    }

    public boolean isAbsolute() {
        return parts.length > 0 && parts[0].isEmpty();
    }

    public boolean isRelativeTo(PurePosixPath other) {
        if (other.parts.length > parts.length) {
            return false;
        }
        for (int i = 0; i < other.parts.length; i++) {
            if (!other.parts[i].equals(parts[i])) {
                return false;
            }
        }
        return true;
    }


    @Override
    public String toString() {
        if (parts.length == 0) {
            return "";
        }
        return String.join("/", parts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PurePosixPath that = (PurePosixPath) o;
        return Arrays.equals(parts, that.parts);
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object[]) parts);
    }

    @Override
    public int compareTo(PurePosixPath other) {
        String thisStr = this.toString();
        String otherStr = other.toString();
        return thisStr.compareTo(otherStr);
    }
}
