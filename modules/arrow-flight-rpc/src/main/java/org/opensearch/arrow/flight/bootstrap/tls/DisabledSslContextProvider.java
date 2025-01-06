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
 * DisabledSslContextProvider is an implementation of the SslContextProvider interface that provides disabled SSL contexts.
 * It is used when SSL is not enabled in the application.
 */
public class DisabledSslContextProvider implements SslContextProvider {

    /**
     * Constructor for DisabledSslContextProvider.
     */
    public DisabledSslContextProvider() {}

    /**
     * Returns false to indicate that SSL is not enabled.
     * @return false
     */
    @Override
    public boolean isSslEnabled() {
        return false;
    }

    /**
     * Returns null as there is no server SSL context when SSL is disabled.
     * @return null
     */
    @Override
    public SslContext getServerSslContext() {
        return null;
    }

    /**
     * Returns null as there is no client SSL context when SSL is disabled.
     * @return null
     */
    @Override
    public SslContext getClientSslContext() {
        return null;
    }
}
