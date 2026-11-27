/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

/**
 * Test-only access to {@link AutoTaggingRegistry}, for integration tests that rebuild clusters in the same JVM
 */
public final class AutoTaggingRegistryTestUtils {

    private AutoTaggingRegistryTestUtils() {}

    /**
     * Replaces the registered feature type with the same name
     * @param featureType the feature type of the current test cluster
     */
    public static void replaceFeatureType(FeatureType featureType) {
        AutoTaggingRegistry.replaceFeatureType(featureType);
    }
}
