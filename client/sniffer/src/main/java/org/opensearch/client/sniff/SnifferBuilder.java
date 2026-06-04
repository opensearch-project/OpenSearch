/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client.sniff;

import org.opensearch.client.RestClient;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Sniffer builder. Helps creating a new {@link Sniffer}.
 */
public final class SnifferBuilder {

    /**
     * The default sniff interval (in milliseconds).
     */
    public static final long DEFAULT_SNIFF_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    /**
     * The default delay of a sniff execution after a failure (in milliseconds).
     */
    public static final long DEFAULT_SNIFF_AFTER_FAILURE_DELAY = TimeUnit.MINUTES.toMillis(1);

    private final RestClient restClient;
    private long sniffIntervalMillis = DEFAULT_SNIFF_INTERVAL;
    private long sniffAfterFailureDelayMillis = DEFAULT_SNIFF_AFTER_FAILURE_DELAY;
    private NodesSniffer nodesSniffer;

    /**
     * Creates a new builder instance by providing the {@link RestClient} that will be used to communicate with opensearch
     */
    SnifferBuilder(RestClient restClient) {
        Objects.requireNonNull(restClient, "restClient cannot be null");
        this.restClient = restClient;
    }

    /**
     * Sets the interval between consecutive ordinary sniff executions in milliseconds. Will be honoured when
     * sniffOnFailure is disabled or when there are no failures between consecutive sniff executions.
     *
     * @param sniffIntervalMillis the interval between sniff executions in milliseconds.
     * @throws IllegalArgumentException if sniffIntervalMillis is not greater than 0
     */
    public SnifferBuilder setSniffIntervalMillis(int sniffIntervalMillis) {
        if (sniffIntervalMillis <= 0) {
            throw new IllegalArgumentException("sniffIntervalMillis must be greater than 0");
        }
        this.sniffIntervalMillis = sniffIntervalMillis;
        return this;
    }

    /**
     * Sets the delay of a sniff execution scheduled after a failure (in milliseconds).
     *
     * @param sniffAfterFailureDelayMillis the sniff delay in milliseconds.
     */
    public SnifferBuilder setSniffAfterFailureDelayMillis(int sniffAfterFailureDelayMillis) {
        if (sniffAfterFailureDelayMillis <= 0) {
            throw new IllegalArgumentException("sniffAfterFailureDelayMillis must be greater than 0");
        }
        this.sniffAfterFailureDelayMillis = sniffAfterFailureDelayMillis;
        return this;
    }

    /**
     * Sets the {@link NodesSniffer} to be used to read hosts. A default instance of {@link OpenSearchNodesSniffer}
     * is created when not provided. This method can be used to change the configuration of the {@link OpenSearchNodesSniffer},
     * or to provide a different implementation (e.g. in case hosts need to taken from a different source).
     *
     * @param nodesSniffer the {@link NodesSniffer} instance to be used.
     */
    public SnifferBuilder setNodesSniffer(NodesSniffer nodesSniffer) {
        Objects.requireNonNull(nodesSniffer, "nodesSniffer cannot be null");
        this.nodesSniffer = nodesSniffer;
        return this;
    }

    /**
     * Creates the {@link Sniffer} based on the provided configuration.
     */
    public Sniffer build() {
        if (nodesSniffer == null) {
            this.nodesSniffer = new OpenSearchNodesSniffer(restClient);
        }
        return new Sniffer(restClient, nodesSniffer, sniffIntervalMillis, sniffAfterFailureDelayMillis);
    }
}
