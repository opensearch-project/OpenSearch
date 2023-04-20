/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.test;

import org.gradle.api.Project;
import org.gradle.api.internal.ConventionMapping;
import org.opensearch.gradle.jvm.JvmTestSuiteHelper;

// Temporary workaround for https://docs.gradle.org/8.1/userguide/upgrading_version_8.html#test_task_default_classpath
interface TestSuiteConventionMappings {
    default void applyConventionMapping(Project project, ConventionMapping conventionMapping) {
        JvmTestSuiteHelper.getDefaultTestSuite(project).ifPresent(defaultTestSuite -> {
            conventionMapping.map("testClassesDirs", () -> { return defaultTestSuite.getSources().getOutput().getClassesDirs(); });

            conventionMapping.map("classpath", () -> { return defaultTestSuite.getSources().getRuntimeClasspath(); });
        });
    }
}
