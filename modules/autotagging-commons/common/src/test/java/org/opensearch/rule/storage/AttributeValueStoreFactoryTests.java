/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.utils.RuleTestUtils.MockRuleAttributes;
import org.opensearch.rule.utils.RuleTestUtils.MockRuleFeatureType;
import org.opensearch.test.OpenSearchTestCase;

public class AttributeValueStoreFactoryTests extends OpenSearchTestCase {
    AttributeValueStoreFactory sut;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        sut = new AttributeValueStoreFactory(MockRuleFeatureType.INSTANCE, DefaultAttributeValueStore::new);
    }

    public void testFeatureLevelStoreInitialisation() {
        for (Attribute attribute : MockRuleFeatureType.INSTANCE.getAllowedAttributesRegistry().values()) {
            assertTrue(sut.getAttributeValueStore(attribute) instanceof DefaultAttributeValueStore<String, String>);
        }
    }

    public void testValidGetAttributeValueStore() {
        assertTrue(
            sut.getAttributeValueStore(MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE) instanceof DefaultAttributeValueStore<String, String>
        );
    }

    public void testInValidGetAttributeValueStore() {
        assertThrows(IllegalArgumentException.class, () -> { sut.getAttributeValueStore(MockRuleAttributes.INVALID_ATTRIBUTE); });
    }
}
