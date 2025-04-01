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
    AttributeValueStoreFactory sut;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        sut = new AttributeValueStoreFactory(WLMFeatureType.WLM, DefaultAttributeValueStore::new);
    }

    public void testFeatureLevelStoreInitialisation() {
        for (Attribute attribute : WLMFeatureType.WLM.getAllowedAttributesRegistry().values()) {
            assertTrue(sut.getAttributeValueStore(attribute) instanceof DefaultAttributeValueStore<String, String>);
        }
    }

    public void testValidGetAttributeValueStore() {
        assertTrue(sut.getAttributeValueStore(TestAttribute.TEST_ATTRIBUTE) instanceof DefaultAttributeValueStore<String, String>);
    }

    public void testInValidGetAttributeValueStore() {
        assertThrows(IllegalArgumentException.class, () -> { sut.getAttributeValueStore(TestAttribute.INVALID_ATTRIBUTE); });
    }
}
