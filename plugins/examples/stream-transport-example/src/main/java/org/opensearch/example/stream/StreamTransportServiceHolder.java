/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.transport.StreamTransportService;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Guice-injectable holder that bridges StreamTransportService from transport actions to REST handlers.
 *
 * StreamTransportService is bound in Guice after plugin components are created, so REST handlers
 * (instantiated by the plugin) can't receive it directly. Transport actions set it here via Guice
 * injection; the plugin passes a supplier from this holder to REST handlers.
 */
public class StreamTransportServiceHolder {
    private final AtomicReference<StreamTransportService> ref;

    public StreamTransportServiceHolder(AtomicReference<StreamTransportService> ref) {
        this.ref = ref;
    }

    public void set(StreamTransportService service) {
        ref.set(service);
    }
}
