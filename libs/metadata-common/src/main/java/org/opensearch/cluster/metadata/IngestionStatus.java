/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Indicates pull-based ingestion status.
 */
@ExperimentalApi
public record IngestionStatus(boolean isPaused) implements Writeable {

    public IngestionStatus(StreamInput in) throws IOException {
        this(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isPaused);
    }

    public static IngestionStatus getDefaultValue() {
        return new IngestionStatus(false);
    }
}
