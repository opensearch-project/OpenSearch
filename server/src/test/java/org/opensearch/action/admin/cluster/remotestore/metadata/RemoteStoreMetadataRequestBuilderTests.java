/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.OpenSearchClient;

import static org.mockito.Mockito.mock;

public class RemoteStoreMetadataRequestBuilderTests extends OpenSearchTestCase {

    public void testSetTimeout() {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);
        RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(mockClient, RemoteStoreMetadataAction.INSTANCE);

        TimeValue timeout = TimeValue.timeValueSeconds(15);
        builder.setTimeout(timeout);

        assertEquals(timeout, builder.request().timeout());
    }

    public void testSetShards() {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);
        RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(mockClient, RemoteStoreMetadataAction.INSTANCE);

        String[] shards = new String[] { "0", "1" };
        builder.setShards(shards);

        assertArrayEquals(shards, builder.request().shards());
    }

    public void testChaining() {
        OpenSearchClient mockClient = mock(OpenSearchClient.class);
        RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(mockClient, RemoteStoreMetadataAction.INSTANCE);

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        String[] shards = new String[] { "0", "2" };

        RemoteStoreMetadataRequestBuilder returned = builder.setTimeout(timeout).setShards(shards);

        assertSame(builder, returned);
        assertEquals(timeout, returned.request().timeout());
        assertArrayEquals(shards, returned.request().shards());
    }
}
