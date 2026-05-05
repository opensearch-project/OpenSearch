/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.transport.StreamTransportResponseHandler;

/**
 * Receive-side base for handlers that consume {@link ArrowBatchResponse}. Pins
 * {@link #skipsDeserialization()} to {@code true} so the Flight transport routes to the native
 * Arrow path.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class ArrowBatchResponseHandler<T extends ArrowBatchResponse> implements StreamTransportResponseHandler<T> {
    /** Constructor. */
    protected ArrowBatchResponseHandler() {}

    @Override
    public final boolean skipsDeserialization() {
        return true;
    }
}
