/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ActionRequestMetadataTests extends OpenSearchTestCase {
    public void testResolvedIndices() {
        ActionRequestMetadata<?, ?> subject = new ActionRequestMetadata<>(
            new TestTransportAction(),
            new TestRequest("my_resolution_result")
        );
        OptionallyResolvedIndices resolvedIndices = subject.resolvedIndices();
        assertEquals(ResolvedIndices.of("my_resolution_result"), resolvedIndices);
    }

    public void testResolvedIndices_unknown() {
        ActionRequestMetadata<?, ?> subject = new ActionRequestMetadata<>(
            new TestTransportActionUnsupported(),
            new TestRequest("my_resolution_result")
        );
        OptionallyResolvedIndices resolvedIndices = subject.resolvedIndices();
        assertFalse(resolvedIndices instanceof ResolvedIndices);
    }

    static class TestTransportAction extends TransportAction<TestRequest, ActionResponse>
        implements
            TransportIndicesResolvingAction<TestRequest> {

        protected TestTransportAction() {
            super("action", new ActionFilters(Set.of()), null);
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<ActionResponse> listener) {

        }

        @Override
        public OptionallyResolvedIndices resolveIndices(TestRequest request) {
            return ResolvedIndices.of(request.resolutionResult);
        }
    }

    static class TestRequest extends ActionRequest {
        final String resolutionResult;

        TestRequest(String resolutionResult) {
            this.resolutionResult = resolutionResult;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class TestTransportActionUnsupported extends TransportAction<TestRequest, ActionResponse> {

        protected TestTransportActionUnsupported() {
            super("action", new ActionFilters(Set.of()), null);
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<ActionResponse> listener) {

        }
    }
}
