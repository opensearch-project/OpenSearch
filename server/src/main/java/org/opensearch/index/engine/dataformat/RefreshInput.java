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

import java.util.ArrayList;
import java.util.List;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public record RefreshInput(List<WriterFileSet> writerFiles, List<Segment> existingSegments) {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<WriterFileSet> writerFiles = new ArrayList<>();
        private List<Segment> existingSegments = List.of();

        public Builder addWriterFileSet(WriterFileSet wfs) {
            writerFiles.add(wfs);
            return this;
        }

        public Builder existingSegments(List<Segment> segments) {
            this.existingSegments = segments;
            return this;
        }

        public RefreshInput build() {
            return new RefreshInput(List.copyOf(writerFiles), existingSegments);
        }
    }
}
