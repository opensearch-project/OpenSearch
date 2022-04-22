/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

public class FeatureFlagTests extends OpenSearchTestCase {

    public void testMissingFeatureFlag() {
        String testFlag = "missingFeatureFlag";
        assertNull(System.getProperty(testFlag));
        assertFalse(FeatureFlags.isEnabled(testFlag));
    }

    public void testNonBooleanFeatureFlag() {
        String javaVersionProperty = "java.version";
        assertNotNull(System.getProperty(javaVersionProperty));
        assertFalse(FeatureFlags.isEnabled(javaVersionProperty));
    }
}
