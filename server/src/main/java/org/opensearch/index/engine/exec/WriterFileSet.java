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
import java.util.Set;

public class WriterFileSet implements Serializable, Writeable {

    private final String directory;
    private final long writerGeneration;
    private final Set<String> files;

    public WriterFileSet(Path directory, long writerGeneration) {
        this.files = new HashSet<>();
        this.writerGeneration = writerGeneration;
        this.directory = directory.toString();
    }

    public WriterFileSet(StreamInput in) throws IOException {
        this.directory = in.readString();
        this.writerGeneration = in.readLong();

        int fileCount = in.readVInt();
        this.files = new HashSet<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            this.files.add(in.readString());
        }
    }

    public WriterFileSet withDirectory(String newDirectory) {
        WriterFileSet newFileSet = new WriterFileSet(Path.of(newDirectory), this.writerGeneration);

        // Extract just the filename and reconstruct with new directory
        for (String oldFilePath : this.files) {
            Path oldPath = Path.of(oldFilePath);
            String fileName = oldPath.getFileName().toString();
            String newFilePath = Path.of(newDirectory, fileName).toString();
            newFileSet.files.add(newFilePath);
        }

        return newFileSet;
    }

    /**
     * Serialize this WriterFileSet to StreamOutput
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(directory);
        out.writeLong(writerGeneration);
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

    /**
     * TODO:
     *
     *
     * equals method was introduced as part of merge.
     * From merge we dont have generation information hence for equals ignoring check for generation for now.
     * We can revisit this later, as today ParquetExecutionEngine stores the WriterFileSet itself, it should hold the
     * FileMetadata as merge might be acting at fileMetadata no writerSet.
     */
    @Override
    public boolean equals(Object o) {
        WriterFileSet other = (WriterFileSet) o;
        return this.directory.equals(other.directory) && this.files.equals(other.files);
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

        public WriterFileSet build() {
            if (directory == null) {
                throw new IllegalStateException("directory must be set");
            }

            if (writerGeneration == null) {
                throw new IllegalStateException("writerGeneration must be set");
            }

            WriterFileSet fileSet = new WriterFileSet(directory, writerGeneration);
            fileSet.files.addAll(this.files);
            return fileSet;
        }
    }
}
