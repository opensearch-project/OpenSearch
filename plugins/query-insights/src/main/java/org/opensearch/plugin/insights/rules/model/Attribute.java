/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

/**
 * Valid attributes for a search query record
 *
 * @opensearch.internal
 */
public enum Attribute {
    /**
     * The search query type
     */
    SEARCH_TYPE,
    /**
     * The search query source
     */
    SOURCE,
    /**
     * Total shards queried
     */
    TOTAL_SHARDS,
    /**
     * The indices involved
     */
    INDICES,
    /**
     * The per phase level latency map for a search query
     */
    PHASE_LATENCY_MAP,
    /**
     * The node id for this request
     */
    NODE_ID,
    /**
     * Custom search request labels
     */
    LABELS;

    /**
     * Read an Attribute from a StreamInput
     *
     * @param in the StreamInput to read from
     * @return Attribute
     * @throws IOException IOException
     */
    static Attribute readFromStream(final StreamInput in) throws IOException {
        return Attribute.valueOf(in.readString().toUpperCase(Locale.ROOT));
    }

    /**
     * Write Attribute to a StreamOutput
     *
     * @param out the StreamOutput to write
     * @param attribute the Attribute to write
     * @throws IOException IOException
     */
    static void writeTo(final StreamOutput out, final Attribute attribute) throws IOException {
        out.writeString(attribute.toString());
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
