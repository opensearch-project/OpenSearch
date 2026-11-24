/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.rule.autotagging.RuleTests.FEATURE_TYPE;
import static org.opensearch.rule.autotagging.RuleTests.INVALID_ATTRIBUTE;
import static org.opensearch.rule.autotagging.RuleTests.TEST_ATTR1_NAME;
import static org.opensearch.rule.autotagging.RuleTests.TestAttribute.TEST_ATTRIBUTE_1;
import static org.mockito.Mockito.mock;

public class FeatureTypeTests extends OpenSearchTestCase {
    public void testIsValidAttribute() {
        assertTrue(FEATURE_TYPE.isValidAttribute(TEST_ATTRIBUTE_1));
        assertFalse(FEATURE_TYPE.isValidAttribute(mock(Attribute.class)));
    }

    public void testGetAttributeFromName() {
        assertEquals(TEST_ATTRIBUTE_1, FEATURE_TYPE.getAttributeFromName(TEST_ATTR1_NAME));
        assertNull(FEATURE_TYPE.getAttributeFromName(INVALID_ATTRIBUTE));
    }

    public void testNamedSerializationWritesOnlyFeatureName() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(FEATURE_TYPE);
            try (var in = out.bytes().streamInput()) {
                assertEquals(FEATURE_TYPE.getName(), in.readString());
                assertEquals(0, in.available());
            }
        }
    }
}
