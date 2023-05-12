/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.util.concurrent.ConcurrentCollections;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

/**
 * Helper class that wraps the lifecycle of setting and finally clearing of
 * a {@link org.opensearch.common.util.FeatureFlags} string.
 */
public class FeatureFlagSetter {

    private static final Logger LOGGER = LogManager.getLogger(FeatureFlagSetter.class);
    private static final Set<String> SET_FLAGS = ConcurrentCollections.newConcurrentSet();

    @SuppressForbidden(reason = "Enables setting of feature flags")
    public static void set(String flag) {
        SET_FLAGS.add(flag);
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(flag, "true"));
        LOGGER.info("set feature_flag={}", flag);
    }

    @SuppressForbidden(reason = "Clears the set feature flags")
    public static void clearAllFlags() {
        for (String flag : SET_FLAGS) {
            AccessController.doPrivileged((PrivilegedAction<String>) () -> System.clearProperty(flag));
        }
        LOGGER.info("unset feature_flags={}", SET_FLAGS);
        SET_FLAGS.clear();
    }
}
