/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.pluginzip;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.UnexpectedBuildFailure;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS;

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

    /**
     * This test is used to verify that adding the 'opensearch.pluginzip' to the project
     * adds some other transitive plugins and tasks under the hood. This is basically
     * a behavioral test of the {@link Publish#apply(Project)} method.
     *
     * This is equivalent of having a build.gradle script with just the following section:
     * <pre>
     *     plugins {
     *       id 'opensearch.pluginzip'
     *     }
     * </pre>
     */
    @Test
    public void applyZipPublicationPluginNoConfig() {
        // All we do here is creating an empty project and applying the Publish plugin.
        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply(Publish.class);

        // WARNING: =====================================================================
        // All the following tests will work only before the gradle project is evaluated.
        // There are some methods that will cause the project to be evaluated, such as:
        // project.getTasksByName()
        // After the project is evaluated there are more tasks found in the project, like
        // the [assemble, build, ...] and other standard tasks.
        // This can potentially break in future gradle versions (?)
        // ===============================================================================

        assertEquals(
            "The Publish plugin is applied which adds total of five tasks from Nebula and MavenPublishing plugins.",
            5,
            project.getTasks().size()
        );

        // Tasks applied from "com.netflix.nebula.maven-base-publish"
        assertTrue(
            project.getTasks()
                .findByName("generateMetadataFileForNebulaPublication") instanceof org.gradle.api.publish.tasks.GenerateModuleMetadata
        );
        assertTrue(
            project.getTasks()
                .findByName("generatePomFileForNebulaPublication") instanceof org.gradle.api.publish.maven.tasks.GenerateMavenPom
        );
        assertTrue(
            project.getTasks()
                .findByName("publishNebulaPublicationToMavenLocal") instanceof org.gradle.api.publish.maven.tasks.PublishToMavenLocal
        );

        // Tasks applied from MavenPublishPlugin
        assertTrue(project.getTasks().findByName("publishToMavenLocal") instanceof org.gradle.api.DefaultTask);
        assertTrue(project.getTasks().findByName("publish") instanceof org.gradle.api.DefaultTask);

        // And we miss the pluginzip publication task (because no publishing was defined for it)
        assertNull(project.getTasks().findByName(ZIP_PUBLISH_TASK));

        // We have the following publishing plugins
        assertEquals(4, project.getPlugins().size());
        // ... of the following types:
        assertNotNull(
            "Project is expected to have OpenSearch pluginzip Publish plugin",
            project.getPlugins().findPlugin(org.opensearch.gradle.pluginzip.Publish.class)
        );
        assertNotNull(
            "Project is expected to have MavenPublishPlugin (applied from OpenSearch pluginzip plugin)",
            project.getPlugins().findPlugin(org.gradle.api.publish.maven.plugins.MavenPublishPlugin.class)
        );
        assertNotNull(
            "Project is expected to have Publishing plugin (applied from MavenPublishPublish plugin)",
            project.getPlugins().findPlugin(org.gradle.api.publish.plugins.PublishingPlugin.class)
        );
        assertNotNull(
            "Project is expected to have nebula MavenNebulaPublishPlugin plugin (applied from OpenSearch pluginzip plugin)",
            project.getPlugins().findPlugin(nebula.plugin.publishing.maven.MavenNebulaPublishPlugin.class)
        );
    }

    /**
     * Verify that if the zip publication is configured then relevant tasks are chained correctly.
     * This test that the dependsOn() is applied correctly.
     */
    @Test
    public void applyZipPublicationPluginWithConfig() throws IOException, URISyntaxException, InterruptedException {

        /* -------------------------------
        // The ideal approach would be to create a project (via ProjectBuilder) with publishzip plugin,
        // have it evaluated (API call) and then check if there are tasks that the plugin uses to hookup into
        // and how these tasks are chained. The problem is that there is a known gradle issue (#20301) that does
        // not allow for it ATM. If, however, it is fixed in the future the following is the code that can
        // be used...

        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply(Publish.class);
        // add publications via API

        // evaluate the project
        ((DefaultProject)project).evaluate();

        // - Check that "validatePluginZipPom" and/or "publishPluginZipPublicationToZipStagingRepository"
        //   tasks have dependencies on "generatePomFileForNebulaPublication".
        // - Check that there is the staging repository added.

        // However, due to known issue(1): https://github.com/gradle/gradle/issues/20301
        // it is impossible to reach to individual tasks and work with them.
        // (1): https://docs.gradle.org/7.4/release-notes.html#known-issues

        // I.e.: The following code throws exception, basically any access to individual tasks fails.
        project.getTasks().getByName("validatePluginZipPom");
         ------------------------------- */

        // Instead, we run the gradle project via GradleRunner (this way we get fully evaluated project)
        // and using the minimal possible configuration (missingPOMEntity) we test that as soon as the zip publication
        // configuration is specified then all the necessary tasks are hooked up and executed correctly.
        // However, this does not test execution order of the tasks.
        GradleRunner runner = prepareGradleRunnerFromTemplate("missingPOMEntity.gradle", ZIP_PUBLISH_TASK/*, "-m"*/);
        BuildResult result = runner.build();

        assertEquals(SUCCESS, result.task(":" + "bundlePlugin").getOutcome());
        assertEquals(SUCCESS, result.task(":" + "generatePomFileForNebulaPublication").getOutcome());
        assertEquals(SUCCESS, result.task(":" + "generatePomFileForPluginZipPublication").getOutcome());
        assertEquals(SUCCESS, result.task(":" + ZIP_PUBLISH_TASK).getOutcome());
    }

    /**
     * If the plugin is used but relevant publication is not defined then a message is printed.
     */
    @Test
    public void missingPublications() throws IOException, URISyntaxException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("missingPublications.gradle", "build", "-m");
        BuildResult result = runner.build();

        assertTrue(result.getOutput().contains("Plugin 'opensearch.pluginzip' is applied but no 'pluginZip' publication is defined."));
    }

    @Test
    public void missingGroupValue() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("missingGroupValue.gradle", "build", ZIP_PUBLISH_TASK);
        Exception e = assertThrows(UnexpectedBuildFailure.class, runner::build);
        assertTrue(e.getMessage().contains("Invalid publication 'pluginZip': groupId cannot be empty."));
    }

    /**
     * This would be the most common use case where user declares Maven publication entity with minimal info
     * and the resulting POM file will use artifactId, groupId and version values based on the Gradle project object.
     */
    @Test
    public void useDefaultValues() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("useDefaultValues.gradle", "build", ZIP_PUBLISH_TASK);
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

        // Parse the maven file and validate default values
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
        assertEquals(model.getArtifactId(), PROJECT_NAME);
        assertNull(model.getName());
        assertNull(model.getDescription());

        assertEquals(model.getUrl(), "https://github.com/doe/sample-plugin");
    }

    /**
     * If the `group` is defined in gradle's allprojects section then it does not have to defined in publications.
     */
    @Test
    public void allProjectsGroup() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("allProjectsGroup.gradle", "build", ZIP_PUBLISH_TASK);
        BuildResult result = runner.build();

        /** Check if build and {@value ZIP_PUBLISH_TASK} tasks have run well */
        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        assertEquals(SUCCESS, result.task(":" + ZIP_PUBLISH_TASK).getOutcome());

        // Parse the maven file and validate default values
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
                        "opensearch",
                        PROJECT_NAME,
                        "2.0.0.0",
                        PROJECT_NAME + "-2.0.0.0.pom"
                    )
                )
            )
        );
        assertEquals(model.getVersion(), "2.0.0.0");
        assertEquals(model.getGroupId(), "org.opensearch");
    }

    /**
     * The groupId value can be defined on several levels. This tests that the most internal level outweighs other levels.
     */
    @Test
    public void groupPriorityLevel() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("groupPriorityLevel.gradle", "build", ZIP_PUBLISH_TASK);
        BuildResult result = runner.build();

        /** Check if build and {@value ZIP_PUBLISH_TASK} tasks have run well */
        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        assertEquals(SUCCESS, result.task(":" + ZIP_PUBLISH_TASK).getOutcome());

        // Parse the maven file and validate default values
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(
            new FileReader(
                new File(
                    projectDir.getRoot(),
                    String.join(
                        File.separator,
                        "build",
                        "local-staging-repo",
                        "level",
                        "3",
                        PROJECT_NAME,
                        "2.0.0.0",
                        PROJECT_NAME + "-2.0.0.0.pom"
                    )
                )
            )
        );
        assertEquals(model.getVersion(), "2.0.0.0");
        assertEquals(model.getGroupId(), "level.3");
    }

    /**
     * In this case the Publication entity is completely missing but still the POM file is generated using the default
     * values including the groupId and version values obtained from the Gradle project object.
     */
    @Test
    public void missingPOMEntity() throws IOException, URISyntaxException, XmlPullParserException {
        GradleRunner runner = prepareGradleRunnerFromTemplate("missingPOMEntity.gradle", "build", ZIP_PUBLISH_TASK);
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
        GradleRunner runner = prepareGradleRunnerFromTemplate("customizedGroupValue.gradle", "build", ZIP_PUBLISH_TASK);
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
        GradleRunner runner = prepareGradleRunnerFromTemplate("customizedInvalidGroupValue.gradle", "build", ZIP_PUBLISH_TASK);
        Exception e = assertThrows(UnexpectedBuildFailure.class, runner::build);
        assertTrue(
            e.getMessage().contains("Invalid publication 'pluginZip': groupId ( ) is not a valid Maven identifier ([A-Za-z0-9_\\-.]+).")
        );
    }

    /**
     * This test verifies that use of the pluginZip does not clash with other maven publication plugins.
     * It covers the case when user calls the "publishToMavenLocal" task.
     */
    @Test
    public void publishToMavenLocal() throws IOException, URISyntaxException, XmlPullParserException {
        // By default, the "publishToMavenLocal" publishes artifacts to a local m2 repo, typically
        // found in `~/.m2/repository`. But this is not practical for this unit test at all. We need to point
        // the 'maven-publish' plugin to a custom m2 repo located in temporary directory associated with this
        // test case instead.
        //
        // According to Gradle documentation this should be possible by proper configuration of the publishing
        // task (https://docs.gradle.org/current/userguide/publishing_maven.html#publishing_maven:install).
        // But for some reason this never worked as expected and artifacts created during this test case
        // were always pushed into the default local m2 repository (ie: `~/.m2/repository`).
        // The only workaround that seems to work is to pass "-Dmaven.repo.local" property via runner argument.
        // (Kudos to: https://stackoverflow.com/questions/72265294/gradle-publishtomavenlocal-specify-custom-directory)
        //
        // The temporary directory that is used as the local m2 repository is created via in task "prepareLocalMVNRepo".
        GradleRunner runner = prepareGradleRunnerFromTemplate(
            "publishToMavenLocal.gradle",
            String.join(File.separator, "-Dmaven.repo.local=" + projectDir.getRoot(), "build", "local-staging-repo"),
            "build",
            "prepareLocalMVNRepo",
            "publishToMavenLocal"
        );
        BuildResult result = runner.build();

        assertEquals(SUCCESS, result.task(":" + "build").getOutcome());
        assertEquals(SUCCESS, result.task(":" + "publishToMavenLocal").getOutcome());

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

        // The "publishToMavenLocal" task will run ALL maven publications, hence we can expect the ZIP publication
        // present as well: https://docs.gradle.org/current/userguide/publishing_maven.html#publishing_maven:tasks
        assertEquals(model.getArtifactId(), PROJECT_NAME);
        assertEquals(model.getGroupId(), "org.custom.group");
        assertEquals(model.getVersion(), "2.0.0.0");
        assertEquals(model.getPackaging(), "zip");

        // We have two publications in the build.gradle file, both are "MavenPublication" based.
        // Both the mavenJava and pluginZip publications publish to the same location (coordinates) and
        // artifacts (the POM file) overwrite each other. However, we can verify that the Zip plugin is
        // the last one and "wins" over the mavenJava.
        assertEquals(model.getDescription(), "pluginZip publication");
    }

    /**
     * A helper method for use cases
     *
     * @param templateName The name of the file (from "pluginzip" folder) to use as a build.gradle for the test
     * @param gradleArguments Optional CLI parameters to pass into Gradle runner
     */
    private GradleRunner prepareGradleRunnerFromTemplate(String templateName, String... gradleArguments) throws IOException,
        URISyntaxException {
        useTemplateFile(projectDir.newFile("build.gradle"), templateName);
        prepareGradleFilesAndSources();

        GradleRunner runner = GradleRunner.create()
            .forwardOutput()
            .withPluginClasspath()
            .withArguments(gradleArguments)
            .withProjectDir(projectDir.getRoot());

        return runner;
    }

    private void prepareGradleFilesAndSources() throws IOException {
        // A dummy "source" file that is processed with bundlePlugin and put into a ZIP artifact file
        File bundleFile = new File(projectDir.getRoot(), PROJECT_NAME + "-source.txt");
        Files.createFile(bundleFile.toPath());
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
