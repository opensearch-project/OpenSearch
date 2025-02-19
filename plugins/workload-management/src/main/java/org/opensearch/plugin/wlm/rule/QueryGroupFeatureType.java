/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.AutoTaggingRegistry;
import org.opensearch.autotagging.FeatureType;

import java.util.Set;

/**
 * Represents the feature type for "query_group" in the OpenSearch workload management plugin.
 * @opensearch.experimental
 */
public class QueryGroupFeatureType implements FeatureType {
    public static final QueryGroupFeatureType INSTANCE = new QueryGroupFeatureType();
    public static final String NAME = "query_group";
    private static final int MAX_ATTRIBUTE_VALUES = 10;
    private static final int MAX_ATTRIBUTE_VALUE_LENGTH = 100;
    private static final Set<Attribute> ALLOWED_ATTRIBUTES = Set.of(QueryGroupAttribute.values());

    private QueryGroupFeatureType() {}

    static {
        INSTANCE.registerFeatureType();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getMaxNumberOfValuesPerAttribute() {
        return MAX_ATTRIBUTE_VALUES;
    }

    @Override
    public int getMaxCharLengthPerAttributeValue() {
        return MAX_ATTRIBUTE_VALUE_LENGTH;
    }

    @Override
    public Set<Attribute> getAllowedAttributes() {
        return ALLOWED_ATTRIBUTES;
    }

    @Override
    public void registerFeatureType() {
        AutoTaggingRegistry.registerFeatureType(INSTANCE);
    }
}
