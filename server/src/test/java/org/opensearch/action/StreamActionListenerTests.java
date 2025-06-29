/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action;

import org.opensearch.core.action.StreamActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for StreamActionListener interface
 */
public class StreamActionListenerTests extends OpenSearchTestCase {
    private TestStreamListener<String> listener;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        listener = new TestStreamListener<>();
    }

    public void testStreamResponseCalls() {
        listener.onStreamResponse("batch1");
        listener.onStreamResponse("batch2");
        listener.onStreamResponse("batch3");

        assertEquals(3, listener.getStreamResponses().size());
        assertEquals("batch1", listener.getStreamResponses().get(0));
        assertEquals("batch2", listener.getStreamResponses().get(1));
        assertEquals("batch3", listener.getStreamResponses().get(2));

        assertNull(listener.getCompleteResponse());
    }

    public void testCompleteResponseCall() {
        listener.onStreamResponse("batch1");
        listener.onStreamResponse("batch2");
        listener.onCompleteResponse("final");

        assertEquals(2, listener.getStreamResponses().size());
        assertEquals("final", listener.getCompleteResponse());
    }

    public void testFailureCall() {
        RuntimeException exception = new RuntimeException("test failure");
        listener.onFailure(exception);

        assertSame(exception, listener.getFailure());
        assertEquals(0, listener.getStreamResponses().size());
        assertNull(listener.getCompleteResponse());
    }

    public void testUnsupportedOnResponseCall() {
        expectThrows(UnsupportedOperationException.class, () -> listener.onResponse("response"));
    }

    /**
     * Simple implementation of StreamActionListener for testing
     */
    public static class TestStreamListener<T> implements StreamActionListener<T> {
        private final List<T> streamResponses = new ArrayList<>();
        private T completeResponse;
        private Exception failure;

        @Override
        public void onStreamResponse(T response) {
            streamResponses.add(response);
        }

        @Override
        public void onCompleteResponse(T response) {
            this.completeResponse = response;
        }

        @Override
        public void onFailure(Exception e) {
            this.failure = e;
        }

        public List<T> getStreamResponses() {
            return streamResponses;
        }

        public T getCompleteResponse() {
            return completeResponse;
        }

        public Exception getFailure() {
            return failure;
        }
    }
}
