/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.junit.BeforeClass;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class FeatureFlagTests extends OpenSearchTestCase {

    @SuppressForbidden(reason = "sets the feature flag")
    @BeforeClass
    public static void enableFeature() {
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(FeatureFlags.REPLICATION_TYPE, "true"));
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(FeatureFlags.REMOTE_STORE, "true"));
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(FeatureFlags.EXTENSIONS, "true"));
    }

    public void testReplicationTypeFeatureFlag() {
        String replicationTypeFlag = FeatureFlags.REPLICATION_TYPE;
        assertNotNull(System.getProperty(replicationTypeFlag));
        assertTrue(FeatureFlags.isEnabled(replicationTypeFlag));
    }

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

    public void testRemoteStoreFeatureFlag() {
        String remoteStoreFlag = FeatureFlags.REMOTE_STORE;
        assertNotNull(System.getProperty(remoteStoreFlag));
        assertTrue(FeatureFlags.isEnabled(remoteStoreFlag));
    }

}
