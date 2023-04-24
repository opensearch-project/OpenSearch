/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.jvm;

import org.gradle.api.Project;
import org.gradle.api.plugins.JvmTestSuitePlugin;
import org.gradle.api.plugins.jvm.JvmTestSuite;
import org.gradle.testing.base.TestSuite;
import org.gradle.testing.base.TestingExtension;

import java.util.Optional;

public final class JvmTestSuiteHelper {
    private JvmTestSuiteHelper() {}

    /**
     * Gets the default test suite. This method assumes the Java plugin is applied,
     * adapted from {@link org.gradle.api.plugins.internal.JavaPluginHelper} since it is not
     * available in Gradle releases predated 8.1.
     */
    public static Optional<JvmTestSuite> getDefaultTestSuite(Project project) {
        TestingExtension testing = project.getExtensions().findByType(TestingExtension.class);
        if (testing == null) {
            return Optional.empty();
        }

        TestSuite defaultTestSuite = testing.getSuites().findByName(JvmTestSuitePlugin.DEFAULT_TEST_SUITE_NAME);
        if (!(defaultTestSuite instanceof JvmTestSuite)) {
            return Optional.empty();
        }

        return Optional.of((JvmTestSuite) defaultTestSuite);
    }
}
