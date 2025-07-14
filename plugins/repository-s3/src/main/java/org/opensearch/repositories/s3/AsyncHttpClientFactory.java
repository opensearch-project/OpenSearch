/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

public class AsyncHttpClientFactory {

    public static final String NETTY_ASYNC_HTTP_CLIENT_TYPE = "netty";
    public static final String CRT_ASYNC_HTTP_CLIENT_TYPE = "crt";

    private final S3ClientSettings clientSettings;
    private final AsyncTransferEventLoopGroup asyncTransferEventLoopGroup;

    public AsyncHttpClientFactory(S3ClientSettings s3ClientSettings, AsyncTransferEventLoopGroup asyncTransferEventLoopGroup) {
        this.clientSettings = s3ClientSettings;
        this.asyncTransferEventLoopGroup = asyncTransferEventLoopGroup;
    }

    public SdkAsyncHttpClient createClient(AsyncHttpClientType clientType) {
        switch (clientType) {
            case NETTY_ASYNC_CLIENT:
                return new NettyAsyncHttpClient(clientSettings, asyncTransferEventLoopGroup).asyncHttpClient();
            case CRT_ASYNC_CLIENT:
                return new CrtAsyncHttpClient(clientSettings).asyncHttpClient();
            default:
                throw new IllegalArgumentException("Invalid client type");
        }
    }

    public enum AsyncHttpClientType {
        NETTY_ASYNC_CLIENT(NETTY_ASYNC_HTTP_CLIENT_TYPE),
        CRT_ASYNC_CLIENT(CRT_ASYNC_HTTP_CLIENT_TYPE);

        private final String name;

        AsyncHttpClientType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        public static AsyncHttpClientType fromClientType(final String clientType) {
            switch (clientType) {
                case NETTY_ASYNC_HTTP_CLIENT_TYPE:
                    return NETTY_ASYNC_CLIENT;
                case CRT_ASYNC_HTTP_CLIENT_TYPE:
                    return CRT_ASYNC_CLIENT;
                default:
                    throw new IllegalArgumentException("Invalid client type");
            }
        }
    }
}
