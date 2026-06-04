/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import reactor.util.annotation.NonNull;

/**
 * Ingest pipeline info help hold the pipeline id and type.
 */
public class IngestPipelineInfo {
    private final String pipelineId;
    private final IngestPipelineType type;

    public IngestPipelineInfo(final @NonNull String pipelineId, final @NonNull IngestPipelineType type) {
        this.pipelineId = pipelineId;
        this.type = type;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public IngestPipelineType getType() {
        return type;
    }

    @Override
    public String toString() {
        return pipelineId + ":" + type.name();
    }
}
