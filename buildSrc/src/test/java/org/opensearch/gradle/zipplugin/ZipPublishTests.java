/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.zipplugin;

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testfixtures.ProjectBuilder;
import org.gradle.api.Project;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Test;
import java.io.IOException;
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository;
import java.io.File;
import org.gradle.testkit.runner.BuildResult;
import java.io.FileWriter;
import java.io.Writer;
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS;
import static org.junit.Assert.assertEquals;
import java.nio.file.Files;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import java.io.FileReader;
import org.gradle.api.tasks.bundling.Zip;

public class ZipPublishTests extends GradleUnitTestCase {

    File testZip = new File("sample-plugin.zip");

    @Test
    public void testZipPublish() throws IOException, XmlPullParserException {
        Project project = ProjectBuilder.builder().build();
        // Apply the opensearch.zippublish plugin
        project.getPluginManager().apply("opensearch.zippublish");
        // Check if the plugin has been applied to the project
        assertTrue(project.getPluginManager().hasPlugin("opensearch.zippublish"));
        // Check if the project has the task from class PublishToMavenRepository after plugin apply
        assertNotNull(project.getTasks().withType(PublishToMavenRepository.class));
        // Create a mock bundlePlugin task
        Zip task = project.getTasks().create("bundlePlugin", Zip.class);
        ZipPublish.configMaven(project);
        // Check if the main task publishMavenzipPublicationToZipstagingRepository exists after plugin apply
        assertTrue(project.getTasks().getNames().contains("publishMavenzipPublicationToZipstagingRepository"));
        assertNotNull("Task to generate: ", project.getTasks().getByName("publishMavenzipPublicationToZipstagingRepository"));
        // Run Gradle functional tests, but calling a build.gradle file, that resembles the plugin publish behavior
        File projectDir = new File("build/functionalTest");
        // Create a sample plugin zip file
        File sampleZip = new File("build/functionalTest/sample-plugin.zip");
        Files.createDirectories(projectDir.toPath());
        Files.createFile(sampleZip.toPath());
        writeString(new File(projectDir, "settings.gradle"), "");
        // Generate the build.gradle file
        String buildFileContent = "apply plugin: 'maven-publish' \n"
            + "publishing {\n"
            + "  repositories {\n"
            + "       maven {\n"
            + "          url = 'local-staging-repo/'\n"
            + "          name = 'zipstaging'\n"
            + "        }\n"
            + "  }\n"
            + "   publications {\n"
            + "         mavenzip(MavenPublication) {\n"
            + "             groupId = 'org.opensearch.plugin' \n"
            + "             artifactId = 'sample-plugin' \n"
            + "             version = '2.0.0.0' \n"
            + "             artifact('sample-plugin.zip') \n"
            + "         }\n"
            + "   }\n"
            + "}";
        writeString(new File(projectDir, "build.gradle"), buildFileContent);
        // Execute the task publishMavenzipPublicationToZipstagingRepository
        GradleRunner runner = GradleRunner.create();
        runner.forwardOutput();
        runner.withPluginClasspath();
        runner.withArguments("publishMavenzipPublicationToZipstagingRepository");
        runner.withProjectDir(projectDir);
        BuildResult result = runner.build();
        // Check if task publishMavenzipPublicationToZipstagingRepository has ran well
        assertEquals(SUCCESS, result.task(":" + "publishMavenzipPublicationToZipstagingRepository").getOutcome());
        // check if the zip has been published to local staging repo
        assertTrue(
            new File("build/functionalTest/local-staging-repo/org/opensearch/plugin/sample-plugin/2.0.0.0/sample-plugin-2.0.0.0.zip")
                .exists()
        );
        // Parse the maven file and validate the groupID to org.opensearch.plugin
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(
            new FileReader("build/functionalTest/local-staging-repo/org/opensearch/plugin/sample-plugin/2.0.0.0/sample-plugin-2.0.0.0.pom")
        );
        String pluginGroupId = "org.opensearch.plugin";
        assertEquals(model.getGroupId(), pluginGroupId);
    }

    private void writeString(File file, String string) throws IOException {
        try (Writer writer = new FileWriter(file)) {
            writer.write(string);
        }
    }

}
