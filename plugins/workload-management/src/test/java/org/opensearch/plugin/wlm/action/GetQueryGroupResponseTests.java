/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetQueryGroupResponseTests extends OpenSearchTestCase {

    public void testSerializationSingleQueryGroup() throws IOException {
        List<QueryGroup> list = new ArrayList<>();
        list.add(QueryGroupTestUtils.queryGroupOne);
        GetQueryGroupResponse response = new GetQueryGroupResponse(list, RestStatus.OK);
        assertEquals(response.getQueryGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetQueryGroupResponse otherResponse = new GetQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        QueryGroupTestUtils.compareQueryGroups(response.getQueryGroups(), otherResponse.getQueryGroups());
    }

    public void testSerializationMultipleQueryGroup() throws IOException {
        GetQueryGroupResponse response = new GetQueryGroupResponse(QueryGroupTestUtils.queryGroupList(), RestStatus.OK);
        assertEquals(response.getQueryGroups(), QueryGroupTestUtils.queryGroupList());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetQueryGroupResponse otherResponse = new GetQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(2, otherResponse.getQueryGroups().size());
        QueryGroupTestUtils.compareQueryGroups(response.getQueryGroups(), otherResponse.getQueryGroups());
    }

    public void testSerializationNull() throws IOException {
        List<QueryGroup> list = new ArrayList<>();
        GetQueryGroupResponse response = new GetQueryGroupResponse(list, RestStatus.OK);
        assertEquals(response.getQueryGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetQueryGroupResponse otherResponse = new GetQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(0, otherResponse.getQueryGroups().size());
    }
}
