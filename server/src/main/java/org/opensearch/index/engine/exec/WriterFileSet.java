/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class WriterFileSet implements Serializable {

    private final String directory;
    private final long writerGeneration;
    private final Set<String> files;

    public WriterFileSet(Path directory, long writerGeneration) {
        this.files = new HashSet<>();
        this.writerGeneration = writerGeneration;
        this.directory = directory.toString();
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
}
