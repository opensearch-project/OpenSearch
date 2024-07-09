/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.apache.lucene.util.ArrayUtil;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
     * Tasks level resource usages in this request
     */
    TASK_RESOURCE_USAGES,
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

    /**
     * Write Attribute value to a StreamOutput
     * @param out the StreamOutput to write
     * @param attributeValue the Attribute value to write
     */
    @SuppressWarnings("unchecked")
    public static void writeValueTo(StreamOutput out, Object attributeValue) throws IOException {
        if (attributeValue instanceof List) {
            out.writeList((List<? extends Writeable>) attributeValue);
        } else {
            out.writeGenericValue(attributeValue);
        }
    }

    /**
     * Read attribute value from the input stream given the Attribute type
     *
     * @param in the {@link StreamInput} input to read
     * @param attribute attribute type to differentiate between Source and others
     * @return parse value
     * @throws IOException IOException
     */
    public static Object readAttributeValue(StreamInput in, Attribute attribute) throws IOException {
        if (attribute == Attribute.TASK_RESOURCE_USAGES) {
            return in.readList(TaskResourceInfo::readFromStream);
        } else {
            return in.readGenericValue();
        }
    }

    /**
     * Read attribute map from the input stream
     *
     * @param in the {@link StreamInput} to read
     * @return parsed attribute map
     * @throws IOException IOException
     */
    public static Map<Attribute, Object> readAttributeMap(StreamInput in) throws IOException {
        int size = readArraySize(in);
        if (size == 0) {
            return Collections.emptyMap();
        }
        Map<Attribute, Object> map = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            Attribute key = readFromStream(in);
            Object value = readAttributeValue(in, key);
            map.put(key, value);
        }
        return map;
    }

    private static int readArraySize(StreamInput in) throws IOException {
        final int arraySize = in.readVInt();
        if (arraySize > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalStateException("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH + " but was: " + arraySize);
        }
        if (arraySize < 0) {
            throw new NegativeArraySizeException("array size must be positive but was: " + arraySize);
        }
        return arraySize;
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
