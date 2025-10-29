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
public class FieldCountStats extends FieldStats {

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
        // Note the readableKey is not used here, as there is no such concept for non-memory stats.
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
