/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.util.TestCapturingListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NoopRemoteRoutingTableServiceTests extends OpenSearchTestCase {
    private NoopRemoteRoutingTableService service;

    @Before
    public void setup() {
        service = new NoopRemoteRoutingTableService();
    }

    public void testGetAsyncIndexRoutingWriteAction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> capturingListener = new TestCapturingListener<>();
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(capturingListener, latch);
        service.getAsyncIndexRoutingWriteAction("clusterUUID", 1, 1, null, listener);
        latch.await(100, TimeUnit.MILLISECONDS);
        assertNull(capturingListener.getResult());
    }

    public void testGetAsyncIndexRoutingReadAction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<IndexRoutingTable> capturingListener = new TestCapturingListener<>();
        LatchedActionListener<IndexRoutingTable> listener = new LatchedActionListener<>(capturingListener, latch);
        service.getAsyncIndexRoutingReadAction("clusterUUID", "filename", listener);
        latch.await(100, TimeUnit.MILLISECONDS);
        assertNull(capturingListener.getResult());
    }

    public void testGetAsyncIndexRoutingDiffWriteAction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> capturingListener = new TestCapturingListener<>();
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(capturingListener, latch);
        service.getAsyncIndexRoutingDiffWriteAction("clusterUUID", 1, 1, null, listener);
        latch.await(100, TimeUnit.MILLISECONDS);
        assertNull(capturingListener.getResult());
    }

    public void testGetAsyncIndexRoutingDiffReadAction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<Diff<RoutingTable>> capturingListener = new TestCapturingListener<>();
        LatchedActionListener<Diff<RoutingTable>> listener = new LatchedActionListener<>(capturingListener, latch);
        service.getAsyncIndexRoutingTableDiffReadAction("clusterUUID", "filename", listener);
        latch.await(100, TimeUnit.MILLISECONDS);
        assertNull(capturingListener.getResult());
    }
}
