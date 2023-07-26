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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.document;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.document.RestGetSourceAction.RestGetSourceResponseListener;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import static java.util.Collections.emptyMap;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;

public class RestGetSourceActionTests extends RestActionTestCase {

    private static RestRequest request = new FakeRestRequest();
    private static FakeRestChannel channel = new FakeRestChannel(request, true, 0);
    private static RestGetSourceResponseListener listener = new RestGetSourceResponseListener(channel, request);

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetSourceAction());
    }

    @AfterClass
    public static void cleanupReferences() {
        request = null;
        channel = null;
        listener = null;
    }

    public void testRestGetSourceAction() throws Exception {
        final BytesReference source = new BytesArray("{\"foo\": \"bar\"}");
        final GetResponse response = new GetResponse(
            new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, true, source, emptyMap(), null)
        );

        final RestResponse restResponse = listener.buildResponse(response);

        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content(), equalTo(new BytesArray("{\"foo\": \"bar\"}")));
    }

    public void testRestGetSourceActionWithMissingDocument() {
        final GetResponse response = new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, false, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Document not found [index1]/[1]"));
    }

    public void testRestGetSourceActionWithMissingDocumentSource() {
        final GetResponse response = new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, true, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Source not found [index1]/[1]"));
    }
}
