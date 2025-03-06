
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.common.ValidationException;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.RuleTests.FEATURE_TYPE;
import static org.opensearch.autotagging.RuleTests.INVALID_ATTRIBUTE;
import static org.opensearch.autotagging.RuleTests.TEST_ATTR1_NAME;
import static org.opensearch.autotagging.RuleTests.TestAttribute.TEST_ATTRIBUTE_1;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class FeatureTypeTests extends OpenSearchTestCase {
    public void testIsValidAttribute() {
        assertTrue(FEATURE_TYPE.isValidAttribute(TEST_ATTRIBUTE_1));
        assertFalse(FEATURE_TYPE.isValidAttribute(mock(Attribute.class)));
    }

    public void testGetAttributeFromName() {
        assertEquals(TEST_ATTRIBUTE_1, FEATURE_TYPE.getAttributeFromName(TEST_ATTR1_NAME));
        assertNull(FEATURE_TYPE.getAttributeFromName(INVALID_ATTRIBUTE));
    }

    public void testValidateAttributeMap() {
        ValidationException validationException = new ValidationException();
        Map<Attribute, Set<String>> validMap = Map.of(TEST_ATTRIBUTE_1, Set.of("value1", "value2"));
        FEATURE_TYPE.validateAttributeMap(validMap, validationException);
        assertTrue(validationException.validationErrors().isEmpty());

        Map<Attribute, Set<String>> invalidMap = Map.of(mock(Attribute.class), Set.of("invalid_value"));
        FEATURE_TYPE.validateAttributeMap(invalidMap, validationException);
        assertFalse(validationException.validationErrors().isEmpty());
    }

    public void testWriteTo() throws IOException {
        StreamOutput mockOutput = mock(StreamOutput.class);
        FEATURE_TYPE.writeTo(mockOutput);
        verify(mockOutput, times(2)).writeString(anyString());
    }
}
