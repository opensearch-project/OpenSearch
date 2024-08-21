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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertEqualQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;

public class CreateQueryGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateQueryGroupRequest.
     */
    public void testSerialization() throws IOException {
        CreateQueryGroupRequest request = new CreateQueryGroupRequest(queryGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateQueryGroupRequest otherRequest = new CreateQueryGroupRequest(streamInput);
        List<QueryGroup> list1 = new ArrayList<>();
        List<QueryGroup> list2 = new ArrayList<>();
        list1.add(queryGroupOne);
        list2.add(otherRequest.getQueryGroup());
        assertEqualQueryGroups(list1, list2);
    }
}
