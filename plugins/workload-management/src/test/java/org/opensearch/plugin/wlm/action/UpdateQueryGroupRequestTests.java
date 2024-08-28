/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.ChangeableQueryGroup;
import org.opensearch.wlm.ChangeableQueryGroup.ResiliencyMode;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertEqualResourceLimits;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;

public class UpdateQueryGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateQueryGroupRequest.
     */
    public void testSerialization() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(NAME_ONE, queryGroupOne.getChangeableQueryGroup());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
        assertEqualResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateQueryGroupRequest with only name field.
     */
    public void testSerializationOnlyName() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(NAME_ONE, new ChangeableQueryGroup(null, new HashMap<>()));
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateQueryGroupRequest with only resourceLimits field.
     */
    public void testSerializationOnlyResourceLimit() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(
            NAME_ONE,
            new ChangeableQueryGroup(null, Map.of(ResourceType.MEMORY, 0.4))
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEqualResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
    }

    /**
     * Tests invalid ResourceType.
     */
    public void testInvalidResourceLimitList() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(
                NAME_ONE,
                new ChangeableQueryGroup(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.3, ResourceType.fromName("random"), 0.4))
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
                new ChangeableQueryGroup(ResiliencyMode.fromName("random"), Map.of(ResourceType.fromName("memory"), 0.3))
            )
        );
    }
}
