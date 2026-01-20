/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WriterFileSet implements Serializable, Writeable {

    private final String directory;
    private final long writerGeneration;
    private final Set<String> files;
    private final long numRows;

    public WriterFileSet(Path directory, long writerGeneration, long numRows) {
        this.numRows = numRows;
        this.files = new HashSet<>();
        this.writerGeneration = writerGeneration;
        this.directory = directory.toString();
    }

    public WriterFileSet(StreamInput in) throws IOException {
        this.directory = in.readString();
        this.writerGeneration = in.readLong();
        this.numRows = in.readVInt();

        int fileCount = in.readVInt();
        this.files = new HashSet<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            this.files.add(in.readString());
        }
    }

    public WriterFileSet withDirectory(String newDirectory) {
        return WriterFileSet.builder()
            .directory(Path.of(newDirectory))
            .writerGeneration(this.writerGeneration)
            .addFiles(this.files)
            .build();
    }

    /**
     * Serialize this WriterFileSet to StreamOutput
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(directory);
        out.writeLong(writerGeneration);
        out.writeVInt((int) numRows);
        out.writeVInt(files.size());
        for (String file : files) {
            out.writeString(file);
        }
    }

    public void add(String file) {
        this.files.add(file);
    }

    public Set<String> getFiles() {
        return files;
    }

    public String getDirectory() {
        return directory;
    }

    public long getNumRows() {
        return numRows;
    }

    public long getTotalSize() {
        return files.stream()
            .mapToLong(file -> {
                try {
                    return java.nio.file.Files.size(Path.of(directory, file));
                } catch (IOException e) {
                    return 0;
                }
            })
            .sum();
    }

    public long getWriterGeneration() {
        return writerGeneration;
    }

    @Override
    public String toString() {
        return "WriterFileSet{" +
            "directory=" + directory +
            ", writerGeneration=" + writerGeneration +
            ", files=" + files +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        WriterFileSet other = (WriterFileSet) o;
        return this.directory.equals(other.directory) && this.files.equals(other.files) && this.getWriterGeneration() == other.getWriterGeneration();
    }

    @Override
    public int hashCode() {
        return this.directory.hashCode() + this.files.hashCode();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Path directory;
        private Long writerGeneration;
        private long numRows;
        private final Set<String> files = new HashSet<>();

        public Builder directory(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder writerGeneration(long writerGeneration) {
            this.writerGeneration = writerGeneration;
            return this;
        }

        public Builder addFile(String file) {
            this.files.add(file);
            return this;
        }

        public Builder addFiles(Set<String> files) {
            this.files.addAll(files);
            return this;
        }

        public Builder addNumRows(long numRows) {
            this.numRows = numRows;
            return this;
        }

        public WriterFileSet build() {
            if (directory == null) {
                throw new IllegalStateException("directory must be set");
            }

            if (writerGeneration == null) {
                throw new IllegalStateException("writerGeneration must be set");
            }

            WriterFileSet fileSet = new WriterFileSet(directory, writerGeneration, numRows);
            fileSet.files.addAll(this.files);
            return fileSet;
        }
    }
}
