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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.mockito.Mockito.mock;

public class UpdateQueryGroupResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        UpdateQueryGroupResponse response = new UpdateQueryGroupResponse(queryGroupOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupResponse otherResponse = new UpdateQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        QueryGroup responseGroup = response.getQueryGroup();
        QueryGroup otherResponseGroup = otherResponse.getQueryGroup();
        List<QueryGroup> list1 = new ArrayList<>();
        List<QueryGroup> list2 = new ArrayList<>();
        list1.add(responseGroup);
        list2.add(otherResponseGroup);
        QueryGroupTestUtils.assertEqualQueryGroups(list1, list2);
    }

    public void testToXContentUpdateSingleQueryGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        UpdateQueryGroupResponse otherResponse = new UpdateQueryGroupResponse(queryGroupOne, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "  \"name\" : \"query_group_one\",\n"
            + "  \"resiliency_mode\" : \"monitor\",\n"
            + "  \"updatedAt\" : 4513232413,\n"
            + "  \"resourceLimits\" : {\n"
            + "    \"memory\" : 0.3\n"
            + "  }\n"
            + "}";
        assertEquals(expected, actual);
    }
}
