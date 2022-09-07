/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gradle.pluginzip;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;

import java.nio.file.Path;
import org.gradle.api.Task;

public class Publish implements Plugin<Project> {

    private static final Logger LOGGER = Logging.getLogger(Publish.class);

    public final static String EXTENSION_NAME = "zipmavensettings";
    public final static String PUBLICATION_NAME = "pluginZip";
    public final static String STAGING_REPO = "zipStaging";
    public final static String PLUGIN_ZIP_PUBLISH_POM_TASK = "generatePomFileForPluginZipPublication";
    public final static String LOCALMAVEN = "publishToMavenLocal";
    public final static String LOCAL_STAGING_REPO_PATH = "/build/local-staging-repo";
    public String zipDistributionLocation = "/build/distributions/";

    public static void configMaven(Project project) {
        final Path buildDirectory = project.getRootDir().toPath();
        project.getPluginManager().apply(MavenPublishPlugin.class);
        project.getExtensions().configure(PublishingExtension.class, publishing -> {
            publishing.repositories(repositories -> {
                repositories.maven(maven -> {
                    maven.setName(STAGING_REPO);
                    maven.setUrl(buildDirectory.toString() + LOCAL_STAGING_REPO_PATH);
                });
            });
            publishing.publications(publications -> {
                MavenPublication mavenZip = (MavenPublication) publications.findByName(PUBLICATION_NAME);

                if (mavenZip == null) {
                    mavenZip = publications.create(PUBLICATION_NAME, MavenPublication.class);
                }

                String groupId = mavenZip.getGroupId();
                if (groupId == null) {
                    // The groupId is not customized thus we get the value from "project.group".
                    // See https://docs.gradle.org/current/userguide/publishing_maven.html#sec:identity_values_in_the_generated_pom
                    groupId = getProperty("group", project);
                }

                String artifactId = project.getName();
                String pluginVersion = getProperty("version", project);
                mavenZip.artifact(project.getTasks().named("bundlePlugin"));
                mavenZip.setGroupId(groupId);
                mavenZip.setArtifactId(artifactId);
                mavenZip.setVersion(pluginVersion);
            });
        });
    }

    static String getProperty(String name, Project project) {
        if (project.hasProperty(name)) {
            Object property = project.property(name);
            if (property != null) {
                return property.toString();
            }
        }
        return null;
    }

    @Override
    public void apply(Project project) {
        project.afterEvaluate(evaluatedProject -> {
            configMaven(project);
            Task validatePluginZipPom = project.getTasks().findByName("validatePluginZipPom");
            if (validatePluginZipPom != null) {
                project.getTasks().getByName("validatePluginZipPom").dependsOn("generatePomFileForNebulaPublication");
            }
            Task publishPluginZipPublicationToZipStagingRepository = project.getTasks()
                .findByName("publishPluginZipPublicationToZipStagingRepository");
            if (publishPluginZipPublicationToZipStagingRepository != null) {
                publishPluginZipPublicationToZipStagingRepository.dependsOn("generatePomFileForNebulaPublication");
            }
        });
    }
}
