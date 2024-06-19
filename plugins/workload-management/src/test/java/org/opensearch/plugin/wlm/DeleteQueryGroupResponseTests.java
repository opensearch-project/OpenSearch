/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;

public class DeleteQueryGroupResponseTests extends OpenSearchTestCase {

    public void testSerializationSingleQueryGroup() throws IOException {
        List<QueryGroup> list = List.of(queryGroupOne);
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(list, RestStatus.OK);
        assertEquals(response.getQueryGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        compareQueryGroups(response.getQueryGroups(), otherResponse.getQueryGroups());
    }

    public void testSerializationMultipleQueryGroup() throws IOException {
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(queryGroupList(), RestStatus.OK);
        assertEquals(response.getQueryGroups(), queryGroupList());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(2, otherResponse.getQueryGroups().size());
        compareQueryGroups(response.getQueryGroups(), otherResponse.getQueryGroups());
    }

    public void testSerializationNull() throws IOException {
        List<QueryGroup> list = new ArrayList<>();
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(list, RestStatus.OK);
        assertEquals(response.getQueryGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(0, otherResponse.getQueryGroups().size());
    }
}
