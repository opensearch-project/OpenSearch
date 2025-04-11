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
import org.gradle.testkit.runner.GradleRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class JavaAgentTests extends GradleUnitTestCase {
    private TemporaryFolder projectDir;
    private final String PROJECT_NAME = "sample-plugin";
    private final String PREPARE_JAVA_AGENT_TASK = "prepareJavaAgent";
    private final String OPENSEARCH_VERSION = "3.0.0";
    private final String BYTEBUDDY_VERSION = "1.12.0";

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

    /**
     * Creates and configures a GradleRunner with the specified template and arguments.
     *
     * @param templateName The name of the build.gradle template to use
     * @param gradleArguments Optional arguments to pass to Gradle
     * @return Configured GradleRunner instance
     */
    private GradleRunner prepareGradleRunnerFromTemplate(String templateName, String... gradleArguments) throws IOException {
        // Create a build.gradle file with the specified template content
        File buildGradleFile = projectDir.newFile("build.gradle");
        String templateContent = getBuildGradleContent(templateName);
        writeString(buildGradleFile, templateContent);

        // Create settings.gradle file with project name
        writeString(projectDir.newFile("settings.gradle"), "rootProject.name = '" + PROJECT_NAME + "'");

        // Create gradle.properties with required versions
        writeString(
            projectDir.newFile("gradle.properties"),
            "opensearch_version=" + OPENSEARCH_VERSION + "\n" + "versions.bytebuddy=" + BYTEBUDDY_VERSION
        );

        // Configure and return a GradleRunner
        return GradleRunner.create().withPluginClasspath().withArguments(gradleArguments).withProjectDir(projectDir.getRoot());
    }

    /**
     * Returns the content for a build.gradle file based on the template name.
     */
    private String getBuildGradleContent(String templateName) {
        if ("basicConfig.gradle".equals(templateName)) {
            return "plugins {\n"
                + "    id 'java'\n"
                + "    id 'opensearch.java-agent'\n"
                + "}\n\n"
                + "repositories {\n"
                + "    mavenCentral()\n"
                + "}\n\n"
                + "dependencies {\n"
                + "    testImplementation 'junit:junit:4.13.2'\n"
                + "}\n";
        } else {
            return "// Empty build file";
        }
    }

    /**
     * Utility method to write a string to a file.
     */
    private void writeString(File file, String content) throws IOException {
        try (Writer writer = new FileWriter(file)) {
            writer.write(content);
        }
    }
}
