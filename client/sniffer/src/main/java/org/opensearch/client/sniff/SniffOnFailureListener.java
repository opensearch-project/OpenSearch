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

import org.opensearch.client.Node;
import org.opensearch.client.RestClient;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link org.opensearch.client.RestClient.FailureListener} implementation that allows to perform
 * sniffing on failure. Gets notified whenever a failure happens and uses a {@link Sniffer} instance
 * to manually reload hosts and sets them back to the {@link RestClient}. The {@link Sniffer} instance
 * needs to be lazily set through {@link #setSniffer(Sniffer)}.
 */
public class SniffOnFailureListener extends RestClient.FailureListener {

    private volatile Sniffer sniffer;
    private final AtomicBoolean set;

    /**
     * Creates a {@code SniffOnFailureListener} instance. The {@link Sniffer} needs to be set
     * through {@link #setSniffer(Sniffer)} after instantiation.
     */
    public SniffOnFailureListener() {
        this.set = new AtomicBoolean(false);
    }

    /**
     * Sets the {@link Sniffer} instance used to perform sniffing.
     *
     * @param sniffer The {@link Sniffer} instance to be used.
     * @throws IllegalStateException if the sniffer was already set, as it can only be set once
     */
    public void setSniffer(Sniffer sniffer) {
        Objects.requireNonNull(sniffer, "sniffer must not be null");
        if (set.compareAndSet(false, true)) {
            this.sniffer = sniffer;
        } else {
            throw new IllegalStateException("sniffer can only be set once");
        }
    }

    @Override
    public void onFailure(Node node) {
        if (sniffer == null) {
            throw new IllegalStateException("sniffer was not set, unable to sniff on failure");
        }
        sniffer.sniffOnFailure();
    }
}
