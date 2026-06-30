/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.agent;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.Objects;

/**
 * Gradle plugin to automatically configure the OpenSearch Java agent
 * for test tasks in OpenSearch plugin projects.
 */
public class JavaAgent implements Plugin<Project> {

    /**
     * Plugin implementation that sets up java agent configuration and applies it to test tasks.
     */
    @Override
    public void apply(Project project) {
        Configuration agentConfiguration = project.getConfigurations().findByName("agent");
        if (agentConfiguration == null) {
            agentConfiguration = project.getConfigurations().create("agent");
        }

        project.afterEvaluate(p -> {
            String opensearchVersion = getOpensearchVersion(p);
            p.getDependencies().add("agent", "org.opensearch:opensearch-agent-bootstrap:" + opensearchVersion);
            p.getDependencies().add("agent", "org.opensearch:opensearch-agent:" + opensearchVersion);
        });

        Configuration finalAgentConfiguration = agentConfiguration;
        TaskProvider<Copy> prepareJavaAgent = project.getTasks().register("prepareJavaAgent", Copy.class, task -> {
            task.from(finalAgentConfiguration);
            task.into(new File(project.getBuildDir(), "agent"));
        });

        project.getTasks().withType(Test.class).configureEach(testTask -> {
            testTask.dependsOn(prepareJavaAgent);

            final String opensearchVersion = getOpensearchVersion(project);

            testTask.doFirst(task -> {
                File agentJar = new File(project.getBuildDir(), "agent/opensearch-agent-" + opensearchVersion + ".jar");

                testTask.jvmArgs("-javaagent:" + agentJar.getAbsolutePath());
            });
        });
    }

    /**
     * Gets the OpenSearch version from project properties, with a fallback default.
     *
     * @param project The Gradle project
     * @return The OpenSearch version to use
     */
    private String getOpensearchVersion(Project project) {
        return Objects.requireNonNull(project.property("opensearch_version")).toString();
    }
}
