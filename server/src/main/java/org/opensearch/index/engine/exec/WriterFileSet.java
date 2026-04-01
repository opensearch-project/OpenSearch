/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a set of files produced by a writer during indexing operations.
 * Groups files by directory and writer generation, tracking metadata such as row count and total size.
 */
@ExperimentalApi
public record WriterFileSet(String directory, long writerGeneration, Set<String> files, long numRows) implements Writeable {

    public WriterFileSet {
        files = Set.copyOf(files);
    }

    /**
     * Constructs a WriterFileSet by deserializing from a {@link StreamInput}.
     */
    public WriterFileSet(StreamInput in, String directory) throws IOException {
        this(directory, in.readLong(), new HashSet<>(in.readStringList()), in.readLong());
    }

    public long getTotalSize() {
        return files.stream().mapToLong(file -> {
            try {
                return java.nio.file.Files.size(Path.of(directory, file));
            } catch (IOException e) {
                return 0;
            }
        }).sum();
    }

    @Override
    public String toString() {
        return "WriterFileSet{" + "directory=" + directory + ", writerGeneration=" + writerGeneration + ", files=" + files + '}';
    }

    /**
     * Serializes this WriterFileSet to the given stream output.
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(writerGeneration);
        out.writeStringCollection(files);
        out.writeLong(numRows);
    }

    /**
     * Creates a new builder for constructing WriterFileSet instances.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing WriterFileSet instances with fluent API.
     */
    @ExperimentalApi
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

            return new WriterFileSet(directory.toString(), writerGeneration, files, numRows);
        }
    }
}
