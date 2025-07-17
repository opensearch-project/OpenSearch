/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.ProxyConfiguration;

import java.time.Duration;

public class CrtAsyncHttpClient implements AsyncHttpClient {

    private final S3ClientSettings clientSettings;

    public CrtAsyncHttpClient(S3ClientSettings s3ClientSettings) {
        this.clientSettings = s3ClientSettings;
    }

    @Override
    public SdkAsyncHttpClient asyncHttpClient() {
        AwsCrtAsyncHttpClient.Builder crtClientBuilder = AwsCrtAsyncHttpClient.builder();

        if (clientSettings.proxySettings.getType() != ProxySettings.ProxyType.DIRECT) {
            ProxyConfiguration.Builder crtProxyConfiguration = ProxyConfiguration.builder();

            crtProxyConfiguration.scheme(clientSettings.proxySettings.getType().toProtocol().toString());
            crtProxyConfiguration.host(clientSettings.proxySettings.getHostName());
            crtProxyConfiguration.port(clientSettings.proxySettings.getPort());
            crtProxyConfiguration.username(clientSettings.proxySettings.getUsername());
            crtProxyConfiguration.password(clientSettings.proxySettings.getPassword());

            crtClientBuilder.proxyConfiguration(crtProxyConfiguration.build());
        }

        crtClientBuilder.connectionTimeout(Duration.ofMillis(clientSettings.connectionTimeoutMillis));
        crtClientBuilder.maxConcurrency(clientSettings.maxConnections);

        return crtClientBuilder.build();
    }
}
