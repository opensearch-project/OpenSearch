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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertEqualQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.mockito.Mockito.mock;

public class DeleteQueryGroupResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of DeleteQueryGroupResponse.
     */
    public void testSerializationSingleQueryGroup() throws IOException {
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(queryGroupOne, RestStatus.OK);
        assertEquals(response.getQueryGroup(), queryGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        List<QueryGroup> list1 = new ArrayList<>();
        List<QueryGroup> list2 = new ArrayList<>();
        list1.add(response.getQueryGroup());
        list2.add(otherResponse.getQueryGroup());
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEqualQueryGroups(list1, list2);
    }

    /**
     * Tests the structure of toXContent of DeleteQueryGroupResponse.
     */
    public void testToXContentDeleteSingleQueryGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(queryGroupOne, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"deleted\" : {\n"
            + "    \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "    \"name\" : \"query_group_one\",\n"
            + "    \"resiliency_mode\" : \"monitor\",\n"
            + "    \"updated_at\" : 4513232413,\n"
            + "    \"resource_limits\" : {\n"
            + "      \"memory\" : 0.3\n"
            + "    }\n"
            + "  }\n"
            + "}";
        assertEquals(expected, actual);
    }
}
