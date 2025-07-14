/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;

import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;

import java.time.Duration;

public class NettyAsyncHttpClient implements AsyncHttpClient {

    private final S3ClientSettings clientSettings;
    private final AsyncTransferEventLoopGroup asyncTransferEventLoopGroup;

    public NettyAsyncHttpClient(S3ClientSettings s3ClientSettings, AsyncTransferEventLoopGroup asyncTransferEventLoopGroup) {
        this.clientSettings = s3ClientSettings;
        this.asyncTransferEventLoopGroup = asyncTransferEventLoopGroup;
    }

    @Override
    public SdkAsyncHttpClient asyncHttpClient() {
        // the response metadata cache is only there for diagnostics purposes,
        // but can force objects from every response to the old generation.
        NettyNioAsyncHttpClient.Builder clientBuilder = NettyNioAsyncHttpClient.builder();

        if (clientSettings.proxySettings.getType() != ProxySettings.ProxyType.DIRECT) {
            ProxyConfiguration.Builder proxyConfiguration = ProxyConfiguration.builder();
            proxyConfiguration.scheme(clientSettings.proxySettings.getType().toProtocol().toString());
            proxyConfiguration.host(clientSettings.proxySettings.getHostName());
            proxyConfiguration.port(clientSettings.proxySettings.getPort());
            proxyConfiguration.username(clientSettings.proxySettings.getUsername());
            proxyConfiguration.password(clientSettings.proxySettings.getPassword());
            clientBuilder.proxyConfiguration(proxyConfiguration.build());
        }

        // TODO: add max retry and UseThrottleRetry. Replace values with settings and put these in default settings
        clientBuilder.connectionTimeout(Duration.ofMillis(clientSettings.connectionTimeoutMillis));
        clientBuilder.connectionAcquisitionTimeout(Duration.ofMillis(clientSettings.connectionAcquisitionTimeoutMillis));
        clientBuilder.maxPendingConnectionAcquires(10_000);
        clientBuilder.maxConcurrency(clientSettings.maxConnections);
        clientBuilder.eventLoopGroup(SdkEventLoopGroup.create(asyncTransferEventLoopGroup.getEventLoopGroup()));
        clientBuilder.tcpKeepAlive(true);

        return clientBuilder.build();
    }
}
