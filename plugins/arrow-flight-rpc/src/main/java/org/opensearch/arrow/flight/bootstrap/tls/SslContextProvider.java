/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import io.netty.handler.ssl.SslContext;

/**
 * Provider interface for SSL/TLS context configuration in OpenSearch Flight.
 */
public interface SslContextProvider {

    /**
     * Gets the SSL context for the Flight server.
     */
    SslContext getServerSslContext();

    /**
     * Gets the SSL context for Flight clients.
     */
    SslContext getClientSslContext();
}
