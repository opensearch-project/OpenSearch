/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.nio.file.Path;
import java.util.Objects;

public class FileSnapshot {
    private final Path path;
    private final long checksum;
    private final byte[] content;
    private final String name;
    private final long contentLength;

    public FileSnapshot(String name, Path path, long checksum, byte[] content) {
        this.name = name;
        this.path = path;
        this.checksum = checksum;
        this.content = content;
        this.contentLength = content.length;
    }

    public Path getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public byte[] getContent() {
        return content;
    }

    public long getChecksum() {
        return checksum;
    }

    public long getContentLength() {
        return contentLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, checksum, contentLength);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileSnapshot other = (FileSnapshot) o;
        return Objects.equals(this.name, other.name)
            && Objects.equals(this.path, other.path)
            && Objects.equals(this.checksum, other.checksum)
            && Objects.equals(this.contentLength, other.contentLength);
    }

    @Override
    public String toString() {
        return new StringBuilder("FileInfo [").append(name).append(path.toUri()).append(checksum).append(contentLength).toString();
    }
}
