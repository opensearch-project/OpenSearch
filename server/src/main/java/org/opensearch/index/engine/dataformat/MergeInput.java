/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.List;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public record MergeInput(List<WriterFileSet> writerFiles, RowIdMapping rowIdMapping, long newWriterGeneration) {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<WriterFileSet> writerFiles;
        private RowIdMapping rowIdMapping;
        private long newWriterGeneration;

        public Builder fileMetadataList(List<WriterFileSet> writerFiles) {
            this.writerFiles = writerFiles;
            return this;
        }

        public Builder rowIdMapping(RowIdMapping rowIdMapping) {
            this.rowIdMapping = rowIdMapping;
            return this;
        }

        public Builder newWriterGeneration(long gen) {
            this.newWriterGeneration = gen;
            return this;
        }

        public MergeInput build() {
            return new MergeInput(writerFiles, rowIdMapping, newWriterGeneration);
        }
    }
}
