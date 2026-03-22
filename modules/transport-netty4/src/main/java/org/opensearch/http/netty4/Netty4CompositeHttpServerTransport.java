/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.http.AbstractHttpServerTransport;
import org.opensearch.http.HttpInfo;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpStats;

import java.io.IOException;

public class Netty4CompositeHttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {
    private final AbstractHttpServerTransport[] transports;

    public Netty4CompositeHttpServerTransport(AbstractHttpServerTransport... transports) {
        if (transports == null || transports.length == 0) {
            throw new IllegalArgumentException("At least one transport must be provided");
        }

        this.transports = transports;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return transports[0].boundAddress(); /* both transports bind to the same port but different protocol */
    }

    @Override
    public HttpInfo info() {
        return transports[0].info();
    }

    @Override
    public HttpStats stats() {
        long serverOpen = 0L, totalOpen = 0L;

        for (AbstractHttpServerTransport transport : transports) {
            final HttpStats stats = transport.stats();
            serverOpen += stats.getServerOpen();
            totalOpen += stats.getTotalOpen();
        }

        return new HttpStats.Builder().serverOpen(serverOpen).totalOpen(totalOpen).build();
    }

    @Override
    protected void doStart() {
        for (AbstractHttpServerTransport transport : transports) {
            transport.start();
        }
    }

    @Override
    protected void doStop() {
        for (AbstractHttpServerTransport transport : transports) {
            transport.stop();
        }
    }

    @Override
    protected void doClose() throws IOException {
        for (AbstractHttpServerTransport transport : transports) {
            IOUtils.closeWhileHandlingException(transport);
        }
    }
}
