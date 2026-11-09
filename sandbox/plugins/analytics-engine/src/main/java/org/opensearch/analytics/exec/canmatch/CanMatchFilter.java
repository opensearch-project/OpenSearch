/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A lightweight predicate representation for can-match evaluation.
 * Each variant carries only what the data node needs to check row-group
 * statistics — column name plus type-specific bounds.
 *
 * <p>Each variant is responsible for serializing/deserializing its own
 * body via {@link #writeBody} and a static {@code readBody} factory.
 * The list-level envelope (FORMAT_VERSION, count, length-prefix per
 * filter) is handled by {@link CanMatchFilterSerializer}.
 *
 * @opensearch.internal
 */
public interface CanMatchFilter {

    /** The column this filter targets. */
    String column();

    /** Type string written to the wire for dispatch on deserialization. */
    String type();

    /** Serialize this filter's type-specific fields (not the type string or length prefix). */
    void writeBody(StreamOutput out) throws IOException;
}
