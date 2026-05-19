/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.agent;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Copy;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class JavaAgentTests extends GradleUnitTestCase {
    private TemporaryFolder projectDir;
    private final String PREPARE_JAVA_AGENT_TASK = "prepareJavaAgent";

    @Before
    public void setUp() throws IOException {
        projectDir = new TemporaryFolder();
        projectDir.create();
    }

    @After
    public void tearDown() {
        projectDir.delete();
    }

    /**
     * This test is used to verify that adding the 'opensearch.java-agent' to the project
     * creates the necessary agent configuration and tasks. This is basically
     * a behavioral test of the {@link JavaAgent#apply(Project)} method.
     */
    @Test
    public void applyJavaAgentPlugin() {
        // Create an empty project and apply the JavaAgent plugin
        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply(JavaAgent.class);

        // Verify the agent configuration was created
        Configuration agentConfig = project.getConfigurations().findByName("agent");
        assertNotNull("Agent configuration should be created", agentConfig);

        // Verify the prepareJavaAgent task was created and is of the right type
        assertNotNull("prepareJavaAgent task should be created", project.getTasks().findByName(PREPARE_JAVA_AGENT_TASK));
        assertTrue("prepareJavaAgent task should be of type Copy", project.getTasks().findByName(PREPARE_JAVA_AGENT_TASK) instanceof Copy);

        // Verify the destination directory of the Copy task
        Copy prepareTask = (Copy) project.getTasks().findByName(PREPARE_JAVA_AGENT_TASK);
        assertEquals(
            "Destination directory should be build/agent",
            new File(project.getBuildDir(), "agent"),
            prepareTask.getDestinationDir()
        );
    }
}
