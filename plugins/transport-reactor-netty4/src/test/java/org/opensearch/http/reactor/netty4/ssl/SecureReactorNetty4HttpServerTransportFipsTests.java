/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4.ssl;

import org.opensearch.common.settings.Settings;
import org.opensearch.http.HttpTransportSettings;

/**
 * FIPS variant of {@link SecureReactorNetty4HttpServerTransportTests}.
 * HTTP3/QUIC is disabled because the native quiche library does not use the JVM FIPS provider.
 */
public class SecureReactorNetty4HttpServerTransportFipsTests extends SecureReactorNetty4HttpServerTransportTests {

    @Override
    protected Settings.Builder createBuilderWithPort() {
        return Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange())
            .put(HttpTransportSettings.SETTING_HTTP_HTTP3_ENABLED.getKey(), false);
    }
}
