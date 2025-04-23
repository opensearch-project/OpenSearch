/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class WorkloadGroupFeatureTypeTests extends OpenSearchTestCase {
    WorkloadGroupFeatureType featureType = WorkloadGroupFeatureType.INSTANCE;

    public void testGetName_returnsCorrectName() {
        assertEquals("workload_group", featureType.getName());
    }

    public void testMaxNumberOfValuesPerAttribute() {
        assertEquals(10, featureType.getMaxNumberOfValuesPerAttribute());
    }

    public void testMaxCharLengthPerAttributeValue() {
        assertEquals(100, featureType.getMaxCharLengthPerAttributeValue());
    }

    public void testGetAllowedAttributesRegistry_containsIndexPattern() {
        Map<String, Attribute> allowedAttributes = featureType.getAllowedAttributesRegistry();
        assertTrue(allowedAttributes.containsKey("index_pattern"));
        assertEquals(RuleAttribute.INDEX_PATTERN, allowedAttributes.get("index_pattern"));
    }

    public void testRegisterFeatureType() {
        AutoTaggingRegistry.registerFeatureType(featureType);
    }
}
