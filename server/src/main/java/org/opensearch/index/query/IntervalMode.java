/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Mode for Text and Mapped Field Types
 *
 * @opensearch.internal
 */
public enum IntervalMode implements Writeable {
    ORDERED(0),
    UNORDERED(1),
    UNORDERED_NO_OVERLAP(2);

    private final int ordinal;

    IntervalMode(int ordinal) {
        this.ordinal = ordinal;
    }

    public static IntervalMode readFromStream(StreamInput in) throws IOException {
        int ord = in.readVInt();
        switch (ord) {
            case (0):
                return ORDERED;
            case (1):
                return UNORDERED;
            case (2):
                return UNORDERED_NO_OVERLAP;
        }
        throw new OpenSearchException("unknown serialized type [" + ord + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal);
    }

    public static IntervalMode fromString(String intervalMode) {
        if (intervalMode == null) {
            throw new IllegalArgumentException("cannot parse mode from null string");
        }

        for (IntervalMode mode : IntervalMode.values()) {
            if (mode.name().equalsIgnoreCase(intervalMode)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("no mode can be parsed from ordinal " + intervalMode);
    }
}
