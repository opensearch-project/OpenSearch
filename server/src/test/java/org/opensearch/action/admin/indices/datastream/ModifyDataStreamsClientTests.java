/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Verifies that the {@code IndicesAdminClient} wiring for the modify-data-stream API routes the request through the
 * client to {@link ModifyDataStreamsAction#INSTANCE}. These code paths are otherwise only exercised by the internal
 * cluster IT, which does not feed the unit-test coverage report.
 */
public class ModifyDataStreamsClientTests extends OpenSearchTestCase {

    private ModifyDataStreamsAction.Request newRequest() {
        return new ModifyDataStreamsAction.Request(List.of(DataStreamAction.addBackingIndex("my-data-stream", "my-index")));
    }

    public void testListenerVariantRoutesToAction() {
        ModifyDataStreamsAction.Request request = newRequest();
        AtomicReference<ActionType<?>> capturedAction = new AtomicReference<>();
        AtomicReference<ActionRequest> capturedRequest = new AtomicReference<>();

        try (NoOpClient client = capturingClient(capturedAction, capturedRequest)) {
            AtomicReference<AcknowledgedResponse> response = new AtomicReference<>();
            client.admin().indices().modifyDataStream(request, ActionListener.wrap(response::set, e -> fail("unexpected failure: " + e)));

            assertThat(capturedAction.get(), sameInstance(ModifyDataStreamsAction.INSTANCE));
            assertThat(capturedRequest.get(), sameInstance(request));
            assertThat(response.get().isAcknowledged(), equalTo(true));
        }
    }

    public void testFutureVariantRoutesToAction() throws Exception {
        ModifyDataStreamsAction.Request request = newRequest();
        AtomicReference<ActionType<?>> capturedAction = new AtomicReference<>();
        AtomicReference<ActionRequest> capturedRequest = new AtomicReference<>();

        try (NoOpClient client = capturingClient(capturedAction, capturedRequest)) {
            AcknowledgedResponse response = client.admin().indices().modifyDataStream(request).get();

            assertThat(capturedAction.get(), sameInstance(ModifyDataStreamsAction.INSTANCE));
            assertThat(capturedRequest.get(), sameInstance(request));
            assertThat(response.isAcknowledged(), equalTo(true));
        }
    }

    public void testCompletionStageVariantRoutesToAction() throws Exception {
        ModifyDataStreamsAction.Request request = newRequest();
        AtomicReference<ActionType<?>> capturedAction = new AtomicReference<>();
        AtomicReference<ActionRequest> capturedRequest = new AtomicReference<>();

        try (NoOpClient client = capturingClient(capturedAction, capturedRequest)) {
            CompletionStage<AcknowledgedResponse> stage = client.admin().indices().modifyDataStreamAsync(request);
            AcknowledgedResponse response = stage.toCompletableFuture().get();

            assertThat(capturedAction.get(), sameInstance(ModifyDataStreamsAction.INSTANCE));
            assertThat(capturedRequest.get(), sameInstance(request));
            assertThat(response.isAcknowledged(), equalTo(true));
        }
    }

    private NoOpClient capturingClient(AtomicReference<ActionType<?>> capturedAction, AtomicReference<ActionRequest> capturedRequest) {
        return new NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                capturedAction.set(action);
                capturedRequest.set(request);
                listener.onResponse((Response) new AcknowledgedResponse(true));
            }
        };
    }
}
