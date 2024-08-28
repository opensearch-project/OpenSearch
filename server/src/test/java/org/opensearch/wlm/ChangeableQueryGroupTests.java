/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.ResourceType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ChangeableQueryGroupTests extends OpenSearchTestCase {

    public void testSerializationDeserialization() throws IOException {
        Map<ResourceType, Double> resourceLimits = new HashMap<>();
        resourceLimits.put(ResourceType.CPU, 0.5);
        resourceLimits.put(ResourceType.MEMORY, 0.75);
        ChangeableQueryGroup changeableQueryGroup = new ChangeableQueryGroup(ChangeableQueryGroup.ResiliencyMode.SOFT, resourceLimits);
        BytesStreamOutput out = new BytesStreamOutput();
        changeableQueryGroup.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChangeableQueryGroup deserializedGroup = new ChangeableQueryGroup(in);
        assertEquals(changeableQueryGroup, deserializedGroup);
    }

    public void testSerializationDeserializationWithNull() throws IOException {
        ChangeableQueryGroup changeableQueryGroup = new ChangeableQueryGroup();
        BytesStreamOutput out = new BytesStreamOutput();
        changeableQueryGroup.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChangeableQueryGroup deserializedGroup = new ChangeableQueryGroup(in);
        assertEquals(0, deserializedGroup.getResourceLimits().size());
        assertEquals(changeableQueryGroup.getResiliencyMode(), deserializedGroup.getResiliencyMode());
    }

    public void testValidateResourceLimits() {
        Map<ResourceType, Double> invalidLimits = new HashMap<>();
        invalidLimits.put(ResourceType.CPU, 1.5);
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> { ChangeableQueryGroup.validateResourceLimits(invalidLimits); }
        );
        String expectedMessage = "resource value should be greater than 0 and less or equal to 1.0";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    public void testSetMethodsWithNullAndEmptyValues() {
        ChangeableQueryGroup queryGroup = new ChangeableQueryGroup();
        queryGroup.setResiliencyMode(null);
        assertNull(queryGroup.getResiliencyMode());
        queryGroup.setResourceLimits(null);
        assertNull(queryGroup.getResourceLimits());
        queryGroup.setResourceLimits(new HashMap<>());
        assertEquals(0, queryGroup.getResourceLimits().size());
    }
}
