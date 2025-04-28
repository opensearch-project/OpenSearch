/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.test.OpenSearchTestCase;

public class RuleAttributeTests extends OpenSearchTestCase {

    public void testGetName() {
        RuleAttribute attribute = RuleAttribute.INDEX_PATTERN;
        assertEquals("index_pattern", attribute.getName());
    }

    public void testFromName() {
        RuleAttribute attribute = RuleAttribute.fromName("index_pattern");
        assertEquals(RuleAttribute.INDEX_PATTERN, attribute);
    }

    public void testFromName_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> RuleAttribute.fromName("invalid_attribute"));
    }
}
