/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.transport.ArrowBatchResponseHandler;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 2)
public class NativeArrowStreamTransportExampleIT extends OpenSearchIntegTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamTransportExamplePlugin.class, FlightStreamPlugin.class);
    }

    @AwaitsFix(bugUrl = "")
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNativeArrowSingleBatch() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);
            List<ReceivedBatch> batches = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> failure = new AtomicReference<>();

            sts.sendRequest(
                node,
                NativeArrowStreamDataAction.NAME,
                new NativeArrowStreamDataRequest(1, 3),
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new NativeArrowResponseHandler(batches, latch, failure)
            );

            assertTrue("Stream should complete within 10s", latch.await(10, TimeUnit.SECONDS));
            assertNull("No exception expected: " + failure.get(), failure.get());
            assertEquals(1, batches.size());

            ReceivedBatch batch = batches.get(0);
            assertEquals(3, batch.rowCount);
            assertEquals("name", batch.fieldNames.get(0));
            assertEquals("age", batch.fieldNames.get(1));
            assertEquals("Alice", batch.names.get(0));
            assertEquals("Bob", batch.names.get(1));
            assertEquals("Carol", batch.names.get(2));
            assertEquals(30, (int) batch.ages.get(0));
            assertEquals(31, (int) batch.ages.get(1));
            assertEquals(32, (int) batch.ages.get(2));
        }
    }

    @AwaitsFix(bugUrl = "")
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNativeArrowMultipleBatches() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);
            List<ReceivedBatch> batches = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> failure = new AtomicReference<>();

            sts.sendRequest(
                node,
                NativeArrowStreamDataAction.NAME,
                new NativeArrowStreamDataRequest(3, 2),
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new NativeArrowResponseHandler(batches, latch, failure)
            );

            assertTrue("Stream should complete within 10s", latch.await(10, TimeUnit.SECONDS));
            assertNull("No exception expected: " + failure.get(), failure.get());
            assertEquals(3, batches.size());

            for (int i = 0; i < 3; i++) {
                ReceivedBatch batch = batches.get(i);
                assertEquals(2, batch.rowCount);
                assertEquals(30, (int) batch.ages.get(0));
                assertEquals(31, (int) batch.ages.get(1));
            }
        }
    }

    /** Deep-copies data from the root since FlightStream reuses it between next() calls. */
    static class ReceivedBatch {
        final int rowCount;
        final List<String> fieldNames;
        final List<String> names;
        final List<Integer> ages;

        ReceivedBatch(VectorSchemaRoot root) {
            this.rowCount = root.getRowCount();
            this.fieldNames = root.getSchema().getFields().stream().map(f -> f.getName()).toList();
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            IntVector ageVector = (IntVector) root.getVector("age");
            this.names = new ArrayList<>();
            this.ages = new ArrayList<>();
            for (int i = 0; i < rowCount; i++) {
                names.add(new String(nameVector.get(i), StandardCharsets.UTF_8));
                ages.add(ageVector.get(i));
            }
        }
    }

    /** Standard handler — read() uses the normal StreamInput contract. */
    static class NativeArrowResponseHandler extends ArrowBatchResponseHandler<NativeArrowStreamDataResponse> {
        private final List<ReceivedBatch> batches;
        private final CountDownLatch latch;
        private final AtomicReference<Exception> failure;

        NativeArrowResponseHandler(List<ReceivedBatch> batches, CountDownLatch latch, AtomicReference<Exception> failure) {
            this.batches = batches;
            this.latch = latch;
            this.failure = failure;
        }

        @Override
        public void handleStreamResponse(StreamTransportResponse<NativeArrowStreamDataResponse> streamResponse) {
            try {
                NativeArrowStreamDataResponse response;
                while ((response = streamResponse.nextResponse()) != null) {
                    batches.add(new ReceivedBatch(response.getRoot()));
                }
                streamResponse.close();
                latch.countDown();
            } catch (Exception e) {
                failure.set(e);
                streamResponse.cancel("Test error", e);
                latch.countDown();
            }
        }

        @Override
        public void handleException(TransportException exp) {
            failure.set(exp);
            latch.countDown();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public NativeArrowStreamDataResponse read(StreamInput in) throws IOException {
            return new NativeArrowStreamDataResponse(in);
        }
    }
}
