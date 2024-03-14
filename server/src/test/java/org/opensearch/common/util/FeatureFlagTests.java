/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;

public class FeatureFlagTests extends OpenSearchTestCase {

    private final String FLAG_PREFIX = "opensearch.experimental.feature.";

    public void testFeatureFlagSet() {
        final String testFlag = FLAG_PREFIX + "testFlag";
        FeatureFlagSetter.set(testFlag);
        assertNotNull(System.getProperty(testFlag));
        assertTrue(FeatureFlags.isEnabled(testFlag));
    }

    public void testMissingFeatureFlag() {
        final String testFlag = FLAG_PREFIX + "testFlag";
        assertNull(System.getProperty(testFlag));
        assertFalse(FeatureFlags.isEnabled(testFlag));
    }

    public void testNonBooleanFeatureFlag() {
        String javaVersionProperty = "java.version";
        assertNotNull(System.getProperty(javaVersionProperty));
        assertFalse(FeatureFlags.isEnabled(javaVersionProperty));
    }
}
