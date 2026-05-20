/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.common.util.FeatureFlags.FEATURE_FLAG_PREFIX;

public class FeatureFlagTests extends OpenSearchTestCase {
    // Evergreen test flag
    private static final String TEST_FLAG = "test.flag.enabled";

    public void testFeatureFlagsNotInitialized() {
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        assertFalse(testFlagsImpl.isEnabled(TEST_FLAG));
    }

    public void testFeatureFlagsFromDefault() {
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        assertFalse(testFlagsImpl.isEnabled(TEST_FLAG));
    }

    public void testFeatureFlagFromEmpty() {
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        testFlagsImpl.initializeFeatureFlags(Settings.EMPTY);
        assertFalse(testFlagsImpl.isEnabled(TEST_FLAG));
    }

    public void testFeatureFlagFromSettings() {
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        testFlagsImpl.initializeFeatureFlags(Settings.builder().put(TEST_FLAG, true).build());
        assertTrue(testFlagsImpl.isEnabled(TEST_FLAG));
        testFlagsImpl.initializeFeatureFlags(Settings.builder().put(TEST_FLAG, false).build());
        assertFalse(testFlagsImpl.isEnabled(TEST_FLAG));
    }

    @SuppressForbidden(reason = "Testing system property functionality")
    private void setSystemPropertyTrue(String key) {
        System.setProperty(key, "true");
    }

    @SuppressForbidden(reason = "Testing system property functionality")
    private String getSystemProperty(String key) {
        return System.getProperty(key);
    }

    @SuppressForbidden(reason = "Testing system property functionality")
    private void clearSystemProperty(String key) {
        System.clearProperty(key);
    }

    public void testNonBooleanFeatureFlag() {
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        String javaVersionProperty = "java.version";
        assertNotNull(getSystemProperty(javaVersionProperty));
        assertFalse(testFlagsImpl.isEnabled(javaVersionProperty));
    }

    public void testFeatureFlagFromSystemProperty() {
        synchronized (TEST_FLAG) { // sync for sys property
            setSystemPropertyTrue(TEST_FLAG);
            FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
            assertTrue(testFlagsImpl.isEnabled(TEST_FLAG));
            clearSystemProperty(TEST_FLAG);
        }
    }

    @SuppressForbidden(reason = "Testing with system property")
    public void testFeatureFlagSettingOverwritesSystemProperties() {
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        synchronized (TEST_FLAG) { // sync for sys property
            setSystemPropertyTrue(TEST_FLAG);
            testFlagsImpl.initializeFeatureFlags(Settings.EMPTY);
            assertTrue(testFlagsImpl.isEnabled(TEST_FLAG));
            clearSystemProperty(TEST_FLAG);
        }
        testFlagsImpl.initializeFeatureFlags(Settings.builder().put(TEST_FLAG, false).build());
        assertFalse(testFlagsImpl.isEnabled(TEST_FLAG));
    }

    @SuppressForbidden(reason = "Testing with system property")
    public void testFeatureDoesNotExist() {
        final String DNE_FF = FEATURE_FLAG_PREFIX + "doesntexist";
        FeatureFlags.FeatureFlagsImpl testFlagsImpl = new FeatureFlags.FeatureFlagsImpl();
        assertFalse(testFlagsImpl.isEnabled(DNE_FF));
        setSystemPropertyTrue(DNE_FF);
        testFlagsImpl.initializeFeatureFlags(Settings.EMPTY);
        assertFalse(testFlagsImpl.isEnabled(DNE_FF));
        clearSystemProperty(DNE_FF);
        testFlagsImpl.initializeFeatureFlags(Settings.builder().put(DNE_FF, true).build());
        assertFalse(testFlagsImpl.isEnabled(DNE_FF));
    }

    /**
     * Test global feature flag instance.
     */

    public void testLockFeatureFlagWithFlagLock() {
        try (FeatureFlags.TestUtils.FlagWriteLock ignore = new FeatureFlags.TestUtils.FlagWriteLock(TEST_FLAG)) {
            assertTrue(FeatureFlags.isEnabled(TEST_FLAG));
            FeatureFlags.initializeFeatureFlags(Settings.builder().put(TEST_FLAG, false).build());
            assertTrue(FeatureFlags.isEnabled(TEST_FLAG)); // flag is locked
        }
    }

    public void testLockFeatureFlagWithHelper() throws Exception {
        FeatureFlags.TestUtils.with(TEST_FLAG, () -> {
            assertTrue(FeatureFlags.isEnabled(TEST_FLAG));
            FeatureFlags.initializeFeatureFlags(Settings.builder().put(TEST_FLAG, false).build());
            assertTrue(FeatureFlags.isEnabled(TEST_FLAG)); // flag is locked
        });
    }

    @LockFeatureFlag(TEST_FLAG)
    public void testLockFeatureFlagAnnotation() {
        assertTrue(FeatureFlags.isEnabled(TEST_FLAG));
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(TEST_FLAG, false).build());
        assertTrue(FeatureFlags.isEnabled(TEST_FLAG)); // flag is locked
    }
}
