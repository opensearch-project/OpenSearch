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
import org.opensearch.search.ResourceType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MEMORY_STRING;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MONITOR_STRING;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.TIMESTAMP_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertEqualResourceLimits;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;

public class UpdateQueryGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateQueryGroupRequest.
     */
    public void testSerialization() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(queryGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
        assertEqualResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateQueryGroupRequest with only name field.
     */
    public void testSerializationOnlyName() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(NAME_ONE, null, new HashMap<>(), TIMESTAMP_ONE);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateQueryGroupRequest with only resourceLimits field.
     */
    public void testSerializationOnlyResourceLimit() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(
            NAME_ONE,
            null,
            Map.of(ResourceType.fromName(MEMORY_STRING), 0.4),
            TIMESTAMP_ONE
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEqualResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    /**
     * Tests invalid ResourceType.
     */
    public void testInvalidResourceLimitList() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(
                NAME_ONE,
                QueryGroup.ResiliencyMode.fromName(MONITOR_STRING),
                Map.of(ResourceType.fromName("memory"), 0.3, ResourceType.fromName(MONITOR_STRING), 0.4),
                TIMESTAMP_ONE
            )
        );
    }

    /**
     * Tests invalid resiliencyMode.
     */
    public void testInvalidEnforcement() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(
                NAME_ONE,
                QueryGroup.ResiliencyMode.fromName("random"),
                Map.of(ResourceType.fromName("memory"), 0.3),
                TIMESTAMP_ONE
            )
        );
    }
}
