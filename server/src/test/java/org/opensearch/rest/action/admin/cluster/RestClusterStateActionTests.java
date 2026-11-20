/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase.VerifyingClient;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RestClusterStateActionTests extends OpenSearchTestCase {

    private final RestClusterStateAction action = new RestClusterStateAction(new SettingsFilter(Collections.emptySet()));

    public void testSerializesOnManagementThread() throws Exception {
        final ThreadRecordingCustom custom = new ThreadRecordingCustom(false);
        try (VerifyingClient client = clientReturning(stateWith(custom), false)) {
            final FakeRestChannel channel = dispatch(client, Collections.emptyMap());

            assertBusy(() -> assertThat(channel.responses().get(), equalTo(1)));
            assertThat(custom.serializingThread.get(), containsString("[" + ThreadPool.Names.MANAGEMENT + "]"));
        }
    }

    public void testResponseHeadersReachTheChannel() throws Exception {
        try (VerifyingClient client = new VerifyingClient(getTestName())) {
            client.setExecuteVerifier((action, req) -> {
                client.threadPool().getThreadContext().addResponseHeader("Warning", "deprecated thing");
                return clusterStateResponse(stateWith(new ThreadRecordingCustom(false)), false);
            });
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/state").build();
            final AtomicReference<List<String>> warnings = new AtomicReference<>();
            final CountDownLatch sent = new CountDownLatch(1);
            action.handleRequest(request, new AbstractRestChannel(request, true) {
                @Override
                public void sendResponse(RestResponse response) {
                    warnings.set(client.threadPool().getThreadContext().getResponseHeaders().get("Warning"));
                    sent.countDown();
                }
            }, client);

            assertTrue(sent.await(10, TimeUnit.SECONDS));
            assertThat(warnings.get(), equalTo(List.of("deprecated thing")));
        }
    }

    public void testWaitForTimedOutIsReported() throws Exception {
        try (VerifyingClient client = clientReturning(stateWith(new ThreadRecordingCustom(false)), true)) {
            final FakeRestChannel channel = dispatch(client, Map.of("wait_for_metadata_version", "5"));

            assertBusy(() -> assertThat(channel.responses().get(), equalTo(1)));
            assertThat(channel.capturedResponse().content().utf8ToString(), containsString("\"wait_for_timed_out\":true"));
        }
    }

    public void testSerializationFailureSendsErrorResponse() throws Exception {
        try (VerifyingClient client = clientReturning(stateWith(new ThreadRecordingCustom(true)), false)) {
            final FakeRestChannel channel = dispatch(client, Collections.emptyMap());

            assertBusy(() -> assertThat(channel.errors().get(), equalTo(1)));
            assertThat(channel.responses().get(), equalTo(0));
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private FakeRestChannel dispatch(VerifyingClient client, Map<String, String> params) throws Exception {
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_cluster/state").withParams(params).build();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        action.handleRequest(request, channel, client);
        return channel;
    }

    private VerifyingClient clientReturning(ClusterState state, boolean waitForTimedOut) {
        final VerifyingClient client = new VerifyingClient(getTestName());
        client.setExecuteVerifier((action, request) -> clusterStateResponse(state, waitForTimedOut));
        return client;
    }

    private static ClusterStateResponse clusterStateResponse(ClusterState state, boolean waitForTimedOut) {
        return new ClusterStateResponse(ClusterName.DEFAULT, state, waitForTimedOut);
    }

    private static ClusterState stateWith(ClusterState.Custom custom) {
        return ClusterState.builder(ClusterName.DEFAULT).putCustom(ThreadRecordingCustom.NAME, custom).build();
    }

    /** Cluster state custom that records the thread that serializes it, and can fail serialization. */
    private static class ThreadRecordingCustom extends AbstractDiffable<ClusterState.Custom> implements ClusterState.Custom {
        static final String NAME = "thread_recording";

        final AtomicReference<String> serializingThread = new AtomicReference<>();
        private final boolean fail;

        ThreadRecordingCustom(boolean fail) {
            this.fail = fail;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            serializingThread.set(Thread.currentThread().getName());
            if (fail) {
                throw new IOException("simulated serialization failure");
            }
            return builder;
        }
    }
}
