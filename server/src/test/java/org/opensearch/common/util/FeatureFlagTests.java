/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.common.util.FeatureFlags.DATETIME_FORMATTER_CACHING;
import static org.opensearch.common.util.FeatureFlags.EXTENSIONS;
import static org.opensearch.common.util.FeatureFlags.IDENTITY;

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

    public void testBooleanFeatureFlagWithDefaultSetToFalse() {
        final String testFlag = IDENTITY;
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
        assertNotNull(testFlag);
        assertFalse(FeatureFlags.isEnabled(testFlag));
    }

    public void testBooleanFeatureFlagInitializedWithEmptySettingsAndDefaultSetToFalse() {
        final String testFlag = DATETIME_FORMATTER_CACHING;
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
        assertNotNull(testFlag);
        assertFalse(FeatureFlags.isEnabled(testFlag));
    }

    public void testInitializeFeatureFlagsWithExperimentalSettings() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(IDENTITY, true).build());
        assertTrue(FeatureFlags.isEnabled(IDENTITY));
        assertFalse(FeatureFlags.isEnabled(DATETIME_FORMATTER_CACHING));
        assertFalse(FeatureFlags.isEnabled(EXTENSIONS));
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }
}
