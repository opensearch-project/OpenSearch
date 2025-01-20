/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * Provider interface for SSL/TLS context configuration in OpenSearch Flight.
 * This interface defines methods for managing SSL contexts for both server and client-side
 * Flight communications.
 */
public interface SslContextProvider {
    /**
     * Checks if SSL/TLS is enabled for Flight communications.
     *
     * @return true if SSL/TLS is enabled, false otherwise
     */
    boolean isSslEnabled();

    /**
     * Gets the SSL context configuration for the Flight server.
     * This context is used to secure incoming connections to the Flight server.
     *
     * @return SslContext configured for server-side TLS
     */
    SslContext getServerSslContext();

    /**
     * Gets the SSL context configuration for Flight clients.
     * This context is used when making outbound connections to other Flight servers.
     *
     * @return SslContext configured for client-side TLS
     */
    SslContext getClientSslContext();
}
