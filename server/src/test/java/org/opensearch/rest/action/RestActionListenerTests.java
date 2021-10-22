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

import org.opensearch.OpenSearchException;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

public class RestActionListenerTests extends OpenSearchTestCase {

    /**
     * Validate that response is sent even when BytesRestResponse can not be constructed from the exception
     * see https://github.com/opensearch-project/OpenSearch/pull/923
     */
    public void testExceptionInByteRestResponse() throws Exception {
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true, 1);
        RestActionListener listener = new RestActionListener(channel) {
            @Override
            protected void processResponse(Object o) {
                fail("call to processResponse is not expected");
            }
        };

        // TODO: it will be better to mock BytesRestResponse() and throw exception from it's ctor, but the current version of
        // mockito does not support mocking static methods and ctor.
        listener.onFailure(new OpenSearchException("mock status() call") {
            @Override
            public RestStatus status() {
                throw new OpenSearchException("call to status failed");
            }
        });

        assertEquals(0, channel.responses().get());
        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, channel.capturedResponse().status());
    }
}
