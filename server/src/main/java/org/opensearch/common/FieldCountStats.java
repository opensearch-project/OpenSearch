/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A reusable class to encode {@code field -&gt; count} mappings
 * Identical to FieldMemoryStats except for toXContent logic
 */
@PublicApi(since = "3.4.0")
public class FieldCountStats extends FieldMemoryStats {

    public FieldCountStats(Map<String, Long> stats) {
        super(stats);
    }

    public FieldCountStats(StreamInput input) throws IOException {
        super(input);
    }

    @Override
    public FieldCountStats copy() {
        return new FieldCountStats(new HashMap<>(stats));
    }

    @Override
    public void toXContent(XContentBuilder builder, String key, String rawKey, String readableKey) throws IOException {
        throw new UnsupportedOperationException("Use toXContent without readableKey argument for FieldCountStats");
    }

    /**
     * Generates x-content into the given builder for each of the fields in this stats instance
     * @param builder the builder to generated on
     * @param key the top level key for this stats object
     * @param rawKey the key for each of the fields' counts
     */
    public void toXContent(XContentBuilder builder, String key, String rawKey) throws IOException {
        builder.startObject(key);
        for (final var entry : stats.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field(rawKey, entry.getValue());
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public void toXContentField(XContentBuilder builder, String field, String rawKey, String readableKey) throws IOException {
        Objects.requireNonNull(rawKey);
        Objects.requireNonNull(field);
        builder.field(rawKey, get(field));
    }
}
