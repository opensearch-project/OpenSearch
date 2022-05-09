/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.pluginzip;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testfixtures.ProjectBuilder;
import org.gradle.api.Project;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.file.Files;

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import java.io.FileReader;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.internal.impldep.org.junit.After;

import java.util.List;
import java.util.ArrayList;

public class PublishTests extends GradleUnitTestCase {
    private TemporaryFolder projectDir;

    @Before
    public void setUp() throws IOException {
        projectDir = new TemporaryFolder();
        projectDir.create();
    }

    @After
    public void tearDown() {
        projectDir.delete();
    }

    @Test
    public void testZipPublish() throws IOException, XmlPullParserException {
        Project project = ProjectBuilder.builder().build();
        String zipPublishTask = "publishPluginZipPublicationToZipStagingRepository";
        // Apply the opensearch.pluginzip plugin
        project.getPluginManager().apply("opensearch.pluginzip");
        // Check if the plugin has been applied to the project
        assertTrue(project.getPluginManager().hasPlugin("opensearch.pluginzip"));
        // Check if the project has the task from class PublishToMavenRepository after plugin apply
        assertNotNull(project.getTasks().withType(PublishToMavenRepository.class));
        // Create a mock bundlePlugin task
        Zip task = project.getTasks().create("bundlePlugin", Zip.class);
        Publish.configMaven(project);
        // Check if the main task publishPluginZipPublicationToZipStagingRepository exists after plugin apply
        assertTrue(project.getTasks().getNames().contains(zipPublishTask));
        assertNotNull("Task to generate: ", project.getTasks().getByName(zipPublishTask));
        // Run Gradle functional tests, but calling a build.gradle file, that resembles the plugin publish behavior

        // Create a sample plugin zip file
        File sampleZip = new File(projectDir.getRoot(), "sample-plugin.zip");
        Files.createFile(sampleZip.toPath());
        writeString(projectDir.newFile("settings.gradle"), "");
        // Generate the build.gradle file
        String buildFileContent = "apply plugin: 'maven-publish' \n"
            + "apply plugin: 'java' \n"
            + "publishing {\n"
            + "  repositories {\n"
            + "       maven {\n"
            + "          url = 'local-staging-repo/'\n"
            + "          name = 'zipStaging'\n"
            + "        }\n"
            + "  }\n"
            + "   publications {\n"
            + "         pluginZip(MavenPublication) {\n"
            + "             groupId = 'org.opensearch.plugin' \n"
            + "             artifactId = 'sample-plugin' \n"
            + "             version = '2.0.0.0' \n"
            + "             artifact('sample-plugin.zip') \n"
            + "         }\n"
            + "   }\n"
            + "}";
        writeString(projectDir.newFile("build.gradle"), buildFileContent);
        // Execute the task publishPluginZipPublicationToZipStagingRepository
        List<String> allArguments = new ArrayList<String>();
        allArguments.add("build");
        allArguments.add(zipPublishTask);
        GradleRunner runner = GradleRunner.create();
        runner.forwardOutput();
        runner.withPluginClasspath();
        runner.withArguments(allArguments);
        runner.withProjectDir(projectDir.getRoot());
        BuildResult result = runner.build();
        // Check if task publishMavenzipPublicationToZipstagingRepository has ran well
        assertEquals(SUCCESS, result.task(":" + zipPublishTask).getOutcome());
        // check if the zip has been published to local staging repo
        assertTrue(
            new File(projectDir.getRoot(), "local-staging-repo/org/opensearch/plugin/sample-plugin/2.0.0.0/sample-plugin-2.0.0.0.zip")
                .exists()
        );
        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        // Parse the maven file and validate the groupID to org.opensearch.plugin
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(
            new FileReader(
                new File(projectDir.getRoot(), "local-staging-repo/org/opensearch/plugin/sample-plugin/2.0.0.0/sample-plugin-2.0.0.0.pom")
            )
        );
        assertEquals(model.getGroupId(), "org.opensearch.plugin");
    }

    private void writeString(File file, String string) throws IOException {
        try (Writer writer = new FileWriter(file)) {
            writer.write(string);
        }
    }

}
