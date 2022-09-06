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
import org.gradle.testkit.runner.UnexpectedBuildFailure;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class PublishTests extends GradleUnitTestCase {
    private TemporaryFolder projectDir;
    private static final String TEMPLATE_RESOURCE_FOLDER = "pluginzip";
    private final String PROJECT_NAME = "sample-plugin";
    private final String ZIP_PUBLISH_TASK = "publishPluginZipPublicationToZipStagingRepository";

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
    public void missingGroupValue() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("missingGroupValue.gradle");
        Exception e = assertThrows(UnexpectedBuildFailure.class, runner::build);
        assertTrue(e.getMessage().contains("Invalid publication 'pluginZip': groupId cannot be empty."));
    }

    /**
     * This would be the most common use case where user declares Maven publication entity with basic info
     * and the resulting POM file will use groupId and version values from the Gradle project object.
     */
    @Test
    public void groupAndVersionValue() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("groupAndVersionValue.gradle");
        BuildResult result = runner.build();

        /** Check if build and {@value ZIP_PUBLISH_TASK} tasks have run well */
        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        assertEquals(SUCCESS, result.task(":" + ZIP_PUBLISH_TASK).getOutcome());

        // check if both the zip and pom files have been published to local staging repo
        assertTrue(
            new File(
                projectDir.getRoot(),
                String.join(
                    File.separator,
                    "build",
                    "local-staging-repo",
                    "org",
                    "custom",
                    "group",
                    PROJECT_NAME,
                    "2.0.0.0",
                    PROJECT_NAME + "-2.0.0.0.pom"
                )
            ).exists()
        );
        assertTrue(
            new File(
                projectDir.getRoot(),
                String.join(
                    File.separator,
                    "build",
                    "local-staging-repo",
                    "org",
                    "custom",
                    "group",
                    PROJECT_NAME,
                    "2.0.0.0",
                    PROJECT_NAME + "-2.0.0.0.zip"
                )
            ).exists()
        );

        // Parse the maven file and validate the groupID
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(
            new FileReader(
                new File(
                    projectDir.getRoot(),
                    String.join(
                        File.separator,
                        "build",
                        "local-staging-repo",
                        "org",
                        "custom",
                        "group",
                        PROJECT_NAME,
                        "2.0.0.0",
                        PROJECT_NAME + "-2.0.0.0.pom"
                    )
                )
            )
        );
        assertEquals(model.getVersion(), "2.0.0.0");
        assertEquals(model.getGroupId(), "org.custom.group");
        assertEquals(model.getUrl(), "https://github.com/doe/sample-plugin");
    }

    /**
     * In this case the Publication entity is completely missing but still the POM file is generated using the default
     * values including the groupId and version values obtained from the Gradle project object.
     */
    @Test
    public void missingPOMEntity() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("missingPOMEntity.gradle");
        BuildResult result = runner.build();

        /** Check if build and {@value ZIP_PUBLISH_TASK} tasks have run well */
        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        assertEquals(SUCCESS, result.task(":" + ZIP_PUBLISH_TASK).getOutcome());

        // Parse the maven file and validate it
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(
            new FileReader(
                new File(
                    projectDir.getRoot(),
                    String.join(
                        File.separator,
                        "build",
                        "local-staging-repo",
                        "org",
                        "custom",
                        "group",
                        PROJECT_NAME,
                        "2.0.0.0",
                        PROJECT_NAME + "-2.0.0.0.pom"
                    )
                )
            )
        );

        assertEquals(model.getArtifactId(), PROJECT_NAME);
        assertEquals(model.getGroupId(), "org.custom.group");
        assertEquals(model.getVersion(), "2.0.0.0");
        assertEquals(model.getPackaging(), "zip");

        assertNull(model.getName());
        assertNull(model.getDescription());

        assertEquals(0, model.getDevelopers().size());
        assertEquals(0, model.getContributors().size());
        assertEquals(0, model.getLicenses().size());
    }

    /**
     * In some cases we need the POM groupId value to be different from the Gradle "project.group" value hence we
     * allow for groupId customization (it will override whatever the Gradle "project.group" value is).
     */
    @Test
    public void customizedGroupValue() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("customizedGroupValue.gradle");
        BuildResult result = runner.build();

        /** Check if build and {@value ZIP_PUBLISH_TASK} tasks have run well */
        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        assertEquals(SUCCESS, result.task(":" + ZIP_PUBLISH_TASK).getOutcome());

        // Parse the maven file and validate the groupID
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(
            new FileReader(
                new File(
                    projectDir.getRoot(),
                    String.join(
                        File.separator,
                        "build",
                        "local-staging-repo",
                        "I",
                        "am",
                        "customized",
                        PROJECT_NAME,
                        "2.0.0.0",
                        PROJECT_NAME + "-2.0.0.0.pom"
                    )
                )
            )
        );

        assertEquals(model.getGroupId(), "I.am.customized");
    }

    /**
     * If the customized groupId value is invalid (from the Maven POM perspective) then we need to be sure it is
     * caught and reported properly.
     */
    @Test
    public void customizedInvalidGroupValue() throws IOException, URISyntaxException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("customizedInvalidGroupValue.gradle");
        Exception e = assertThrows(UnexpectedBuildFailure.class, runner::build);
        assertTrue(
            e.getMessage().contains("Invalid publication 'pluginZip': groupId ( ) is not a valid Maven identifier ([A-Za-z0-9_\\-.]+).")
        );
    }

    private GradleRunner prepareGradleRunnerFromTemplate(String templateName) throws IOException, URISyntaxException {
        useTemplateFile(projectDir.newFile("build.gradle"), templateName);
        prepareGradleFilesAndSources();

        GradleRunner runner = GradleRunner.create()
            .forwardOutput()
            .withPluginClasspath()
            .withArguments("build", ZIP_PUBLISH_TASK)
            .withProjectDir(projectDir.getRoot());

        return runner;
    }

    private void prepareGradleFilesAndSources() throws IOException {
        // A dummy "source" file that is processed with bundlePlugin and put into a ZIP artifact file
        File bundleFile = new File(projectDir.getRoot(), PROJECT_NAME + "-source.txt");
        Path zipFile = Files.createFile(bundleFile.toPath());
        // Setting a project name via settings.gradle file
        writeString(projectDir.newFile("settings.gradle"), "rootProject.name = '" + PROJECT_NAME + "'");
    }

    private void writeString(File file, String string) throws IOException {
        try (Writer writer = new FileWriter(file)) {
            writer.write(string);
        }
    }

    /**
     * Write the content of the "template" file into the target file.
     * The template file must be located in the {@value TEMPLATE_RESOURCE_FOLDER} folder.
     * @param targetFile A target file
     * @param templateFile A name of the template file located under {@value TEMPLATE_RESOURCE_FOLDER} folder
     */
    private void useTemplateFile(File targetFile, String templateFile) throws IOException, URISyntaxException {

        URL resource = getClass().getClassLoader().getResource(String.join(File.separator, TEMPLATE_RESOURCE_FOLDER, templateFile));
        Path resPath = Paths.get(resource.toURI()).toAbsolutePath();
        List<String> lines = Files.readAllLines(resPath, StandardCharsets.UTF_8);

        try (Writer writer = new FileWriter(targetFile)) {
            for (String line : lines) {
                writer.write(line);
                writer.write(System.lineSeparator());
            }
        }
    }

}
