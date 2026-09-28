/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.model;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class GetTieringStatusRequestTests extends OpenSearchTestCase {

    public void testInvalidRequest() {
        GetTieringStatusRequest request = new GetTieringStatusRequest();
        ActionRequestValidationException validate = request.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.getMessage(), containsString("index is missing"));
    }

    public void testSerialization() throws IOException {
        final GetTieringStatusRequest request = new GetTieringStatusRequest();
        request.setIndex("foo");
        request.setDetailedFlagEnabled(Boolean.TRUE);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                GetTieringStatusRequest newRequest = new GetTieringStatusRequest(in);
                assertEquals("foo", newRequest.getIndex());
                assertEquals(true, newRequest.getDetailedFlag());
            }
        }
    }
}
