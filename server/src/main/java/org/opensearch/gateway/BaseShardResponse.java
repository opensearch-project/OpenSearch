/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Base response class for shard response. Provides necessary information about shard level response. Based on these
 * functionalities, receiver decides if it needs to store the response or ignore it or retry the fetch.
 *
 * @opensearch.internal
 */
public abstract class BaseShardResponse extends TransportResponse {
    public BaseShardResponse(){}

    public abstract boolean isEmpty();

    public abstract Exception getException();

    public BaseShardResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }
}
