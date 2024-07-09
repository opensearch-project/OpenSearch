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

public class CreateQueryGroupResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        CreateQueryGroupResponse response = new CreateQueryGroupResponse(QueryGroupTestUtils.queryGroupOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateQueryGroupResponse otherResponse = new CreateQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        QueryGroup responseGroup = response.getQueryGroup();
        QueryGroup otherResponseGroup = otherResponse.getQueryGroup();
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(responseGroup);
        listTwo.add(otherResponseGroup);
        QueryGroupTestUtils.compareQueryGroups(listOne, listTwo);
    }
}
