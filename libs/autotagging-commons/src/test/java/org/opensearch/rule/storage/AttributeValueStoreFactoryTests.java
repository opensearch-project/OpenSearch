/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.autotagging.Attribute;
import org.opensearch.rule.InMemoryRuleProcessingServiceTests.TestAttribute;
import org.opensearch.rule.InMemoryRuleProcessingServiceTests.WLMFeatureType;
import org.opensearch.test.OpenSearchTestCase;

public class AttributeValueStoreFactoryTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AttributeValueStoreFactory.init(WLMFeatureType.WLM, () -> new DefaultAttributeValueStore<>());
    }

    public void testFeatureLevelStoreInitialisation() {
        for (Attribute attribute : WLMFeatureType.WLM.getAllowedAttributesRegistry().values()) {
            assertTrue(AttributeValueStoreFactory.getAttributeValueStore(attribute) instanceof DefaultAttributeValueStore<String, String>);
        }
    }

    public void testValidGetAttributeValueStore() {
        assertTrue(
            AttributeValueStoreFactory.getAttributeValueStore(TestAttribute.TEST_ATTRIBUTE) instanceof DefaultAttributeValueStore<
                String,
                String>
        );
    }

    public void testInValidGetAttributeValueStore() {
        assertThrows(
            IllegalArgumentException.class,
            () -> { AttributeValueStoreFactory.getAttributeValueStore(TestAttribute.INVALID_ATTRIBUTE); }
        );
    }
}
