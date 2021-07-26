/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.instanceOf;

public class RestActionListenerTests extends OpenSearchTestCase {
    private static class SimpleExceptionRestChannel extends AbstractRestChannel {
        private RestResponse lastResponse = null; 

        public RestResponse getLastResponse() {
            return lastResponse;
        }

        SimpleExceptionRestChannel(RestRequest request) {
            super(request, false);
        }

        @Override
        public void sendResponse(RestResponse response) {
            this.lastResponse = response;
        }
    }

    /**
     * Ensure that first failure to construct a BytesRestResponse is handled.
     * see https://github.com/opensearch-project/OpenSearch/pull/923
     */
    public void testWrapsException() {
        RestRequest request = new FakeRestRequest();
        SimpleExceptionRestChannel channel = new SimpleExceptionRestChannel(request);

        class DummyActionListener extends RestActionListener<ActionResponse> {
            protected DummyActionListener(RestChannel channel) {
                super(channel);
            }
    
            private int exceptionCount = 0;
    
            public int getExceptionCount() {
                return exceptionCount;
            }
    
            @Override
            protected BytesRestResponse getRestResponse(Exception e) throws IOException {
                if (exceptionCount++ == 1) {
                    throw new IOException();
                } else {
                    return super.getRestResponse(e);
                }
            }
    
            @Override
            protected void processResponse(ActionResponse response) throws Exception {
    
            }        
        }

        DummyActionListener listener = new DummyActionListener(channel);
        listener.onFailure(new Exception());
        assertThat(channel.getLastResponse(), instanceOf(BytesRestResponse.class));
        assertEquals(1, listener.getExceptionCount());
    }    
}