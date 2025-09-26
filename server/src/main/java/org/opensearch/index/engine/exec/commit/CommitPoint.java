/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

public final class CommitPoint {

    private final String commitFileName;
    private final long generation;
    private final Collection<String> fileNames;
    private final Path directory;
    private final Map<String, String> commitData;

    private CommitPoint(Builder builder) {
        this.commitFileName = builder.commitFileName;
        this.generation = builder.generation;
        this.fileNames = builder.fileNames;
        this.directory = builder.directory;
        this.commitData = builder.commitData;
    }

    public String getCommitFileName() {
        return commitFileName;
    }

    public long getGeneration() {
        return generation;
    }

    public Collection<String> getFileNames() {
        return fileNames;
    }

    public Path getDirectory() {
        return directory;
    }

    public Map<String, String> getCommitData() {
        return commitData;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String commitFileName;
        private long generation;
        private Collection<String> fileNames;
        private Path directory;
        private Map<String, String> commitData;

        private Builder() {
        }

        public Builder commitFileName(String commitFileName) {
            this.commitFileName = commitFileName;
            return this;
        }

        public Builder generation(long generation) {
            this.generation = generation;
            return this;
        }

        public Builder fileNames(Collection<String> fileNames) {
            this.fileNames = fileNames;
            return this;
        }

        public Builder directory(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder commitData(Map<String, String> commitData) {
            this.commitData = commitData;
            return this;
        }

        public CommitPoint build() {
            return new CommitPoint(this);
        }
    }

}
