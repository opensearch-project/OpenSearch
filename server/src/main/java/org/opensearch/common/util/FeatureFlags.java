/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * Utility class to manage feature flags. Feature flags are system properties that must be set on the JVM.
 * These are used to gate the visibility/availability of incomplete features. Fore more information, see
 * https://featureflags.io/feature-flag-introduction/
 *
 * @opensearch.internal
 */
public class FeatureFlags {

    /**
     * Gates the visibility of the index setting that allows changing of replication type.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String REPLICATION_TYPE = "opensearch.experimental.feature.replication_type.enabled";

    /**
     * Gates the visibility of the index setting that allows persisting data to remote store along with local disk.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String REMOTE_STORE = "opensearch.experimental.feature.remote_store.enabled";

    /**
     * Gates the functionality of a new parameter to the snapshot restore API
     * that allows for creation of a new index type that searches a snapshot
     * directly in a remote repository without restoring all index data to disk
     * ahead of time.
     */
    public static final String SEARCHABLE_SNAPSHOT = "opensearch.experimental.feature.searchable_snapshot.enabled";

    /**
     * Used to test feature flags whose values are expected to be booleans.
     * This method returns true if the value is "true" (case-insensitive),
     * and false otherwise.
     */
    public static boolean isEnabled(String featureFlagName) {
        return "true".equalsIgnoreCase(System.getProperty(featureFlagName));
    }
}
