/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.transport;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Marker for a {@link org.opensearch.core.common.io.stream.StreamInput} that carries a
 * native Arrow batch rather than bytes. Implemented by the transport's native-Arrow
 * {@code StreamInput}; consumed by {@link ArrowBatchResponse#ArrowBatchResponse(
 * org.opensearch.core.common.io.stream.StreamInput)} to claim the batch without going
 * through byte deserialization.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ArrowStreamInput {

    /**
     * Returns the Arrow batch delivered with this input.
     *
     * @return the batch
     */
    VectorSchemaRoot getRoot();

    /**
     * Transfers ownership of the batch from this input to the caller. After this call the
     * input's {@code close()} must not release the batch's buffers.
     */
    void claimOwnership();
}
