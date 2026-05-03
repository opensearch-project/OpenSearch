/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Phases an in-flight catalog publish moves through. Serialized by ordinal — do not reorder.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum PublishPhase implements Writeable {

    INITIALIZED,
    PUBLISHING,
    FINALIZING_SUCCESS,
    FINALIZING_FAILURE,
    /** Terminal. Entry is kept so operators can read {@code lastFailureReason}. */
    FAILED;

    public static PublishPhase readFrom(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        PublishPhase[] values = values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IOException("unknown PublishPhase ordinal [" + ordinal + "]");
        }
        return values[ordinal];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(ordinal());
    }
}
