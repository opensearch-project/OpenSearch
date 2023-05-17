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

    private static FeatureFlagSetter INSTANCE = null;

    private static synchronized FeatureFlagSetter getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new FeatureFlagSetter();
        }
        return INSTANCE;
    }

    public static synchronized void set(String flag) {
        getInstance().setFlag(flag);
    }

    public static synchronized void clear() {
        if (INSTANCE != null) {
            INSTANCE.clearAll();
            INSTANCE = null;
        }
    }

    private static final Logger LOGGER = LogManager.getLogger(FeatureFlagSetter.class);
    private final Set<String> flags = ConcurrentCollections.newConcurrentSet();

    @SuppressForbidden(reason = "Enables setting of feature flags")
    private void setFlag(String flag) {
        flags.add(flag);
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(flag, "true"));
        LOGGER.info("set feature_flag={}", flag);
    }

    @SuppressForbidden(reason = "Clears the set feature flags")
    private void clearAll() {
        for (String flag : flags) {
            AccessController.doPrivileged((PrivilegedAction<String>) () -> System.clearProperty(flag));
        }
        LOGGER.info("unset feature_flags={}", flags);
        flags.clear();
    }
}
