/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.junit.Before;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.test.OpenSearchTestCase;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import static org.mockito.Mockito.mock;

public class AsyncHttpClientFactoryTests extends OpenSearchTestCase {

    private S3ClientSettings clientSettings;
    private AsyncTransferEventLoopGroup eventLoopGroup;
    private AsyncHttpClientFactory factory;

    @Before
    public void setup() {
        clientSettings = mock(S3ClientSettings.class);
        eventLoopGroup = mock(AsyncTransferEventLoopGroup.class);
        factory = new AsyncHttpClientFactory(clientSettings, eventLoopGroup);
    }

    public void testCreateNettyClient() {
        SdkAsyncHttpClient client = factory.createClient(AsyncHttpClientFactory.AsyncHttpClientType.NETTY_ASYNC_CLIENT);
        assertNotNull(client);
        assertTrue(client instanceof NettyNioAsyncHttpClient);
    }

    public void testCreateCrtClient() {
        SdkAsyncHttpClient client = factory.createClient(AsyncHttpClientFactory.AsyncHttpClientType.CRT_ASYNC_CLIENT);
        assertNotNull(client);
        assertTrue(client instanceof AwsCrtAsyncHttpClient);
    }

    public void testInvalidClientType() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> AsyncHttpClientFactory.AsyncHttpClientType.fromClientType("invalid")
        );
        assertEquals("Invalid client type", exception.getMessage());
    }

    public void testClientTypeFromString() {
        assertEquals(
            AsyncHttpClientFactory.AsyncHttpClientType.NETTY_ASYNC_CLIENT,
            AsyncHttpClientFactory.AsyncHttpClientType.fromClientType("netty")
        );
        assertEquals(
            AsyncHttpClientFactory.AsyncHttpClientType.CRT_ASYNC_CLIENT,
            AsyncHttpClientFactory.AsyncHttpClientType.fromClientType("crt")
        );
    }

    public void testClientTypeToString() {
        assertEquals("netty", AsyncHttpClientFactory.AsyncHttpClientType.NETTY_ASYNC_CLIENT.toString());
        assertEquals("crt", AsyncHttpClientFactory.AsyncHttpClientType.CRT_ASYNC_CLIENT.toString());
    }
}
