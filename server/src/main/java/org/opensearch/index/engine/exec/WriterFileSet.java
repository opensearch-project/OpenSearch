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
}
