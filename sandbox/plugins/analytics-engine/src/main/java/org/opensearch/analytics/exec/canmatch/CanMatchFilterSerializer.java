/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Serializes and deserializes a list of {@link CanMatchFilter} instances
 * to/from a byte array for transport over the wire.
 *
 * <p>Each filter variant owns its body serialization via
 * {@link CanMatchFilter#writeBody} and a static {@code readBody} factory.
 * This codec handles the list-level envelope: FORMAT_VERSION, count, and
 * the per-filter length-prefix + type-string wrapper that enables
 * forward-compatible skipping of unknown types.
 *
 * <p>Wire layout:
 * <pre>
 * vInt   FORMAT_VERSION
 * vInt   filterCount
 * repeat filterCount:
 *   vInt    byteLength    (total bytes for this filter — enables skip on unknown type)
 *   string  type          ("LongRange", etc.)
 *   ...type-specific body fields (written by the filter's writeBody)...
 * </pre>
 *
 * <p>Forward compatibility:
 * <ul>
 *   <li>Unknown FORMAT_VERSION → return empty list (fail-open, no pruning)</li>
 *   <li>Unknown type string → skip byteLength bytes, continue to next filter</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class CanMatchFilterSerializer {

    private static final Logger logger = LogManager.getLogger(CanMatchFilterSerializer.class);

    private static final int FORMAT_VERSION = 1;

    private CanMatchFilterSerializer() {}

    /**
     * Serializes a list of filters to a byte array.
     * Empty or null list produces a zero-length array.
     */
    public static byte[] serialize(List<CanMatchFilter> filters) throws IOException {
        if (filters == null || filters.isEmpty()) {
            return new byte[0];
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(FORMAT_VERSION);
            out.writeVInt(filters.size());
            for (CanMatchFilter filter : filters) {
                writeFilter(out, filter);
            }
            return BytesReference.toBytes(out.bytes());
        }
    }

    /**
     * Deserializes a byte array back into a list of filters.
     * Zero-length or null input produces an empty list.
     * Unknown format version produces an empty list (fail-open).
     * Unknown filter types are skipped (fail-open per filter).
     */
    public static List<CanMatchFilter> deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return Collections.emptyList();
        }
        try (StreamInput in = new BytesArray(bytes).streamInput()) {
            int version = in.readVInt();
            if (version != FORMAT_VERSION) {
                logger.debug("Unknown CanMatchFilter format version {}; returning empty (fail-open)", version);
                return Collections.emptyList();
            }
            int count = in.readVInt();
            List<CanMatchFilter> filters = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                CanMatchFilter filter = readFilter(in);
                if (filter != null) {
                    filters.add(filter);
                }
            }
            return filters;
        }
    }

    private static void writeFilter(BytesStreamOutput out, CanMatchFilter filter) throws IOException {
        try (BytesStreamOutput tmp = new BytesStreamOutput()) {
            tmp.writeString(filter.type());
            filter.writeBody(tmp);
            int length = tmp.size();
            out.writeVInt(length);
            out.writeBytes(BytesReference.toBytes(tmp.bytes()), 0, length);
        }
    }

    private static CanMatchFilter readFilter(StreamInput in) throws IOException {
        int byteLength = in.readVInt();
        int availableBefore = in.available();

        String type = in.readString();
        CanMatchFilter filter = switch (type) {
            case LongRange.TYPE -> LongRange.readBody(in);
            default -> {
                logger.debug("Unknown CanMatchFilter type '{}'; skipping (fail-open)", type);
                yield null;
            }
        };

        // Skip any remaining bytes for this filter (handles unknown types
        // and future fields appended to known types)
        int bytesConsumed = availableBefore - in.available();
        int remaining = byteLength - bytesConsumed;
        if (remaining > 0) {
            byte[] skip = new byte[remaining];
            in.readBytes(skip, 0, remaining);
        }

        return filter;
    }
}
