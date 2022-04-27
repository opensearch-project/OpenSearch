/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gradle.pluginzip;

import java.util.*;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import java.nio.file.Path;

public class Publish implements Plugin<Project> {
    private Project project;

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
                publications.create(PUBLICATION_NAME, MavenPublication.class, mavenZip -> {
                    String zipGroup = "org.opensearch.plugin";
                    String zipArtifact = project.getName();
                    String zipVersion = getProperty("version", project);
                    mavenZip.artifact(project.getTasks().named("bundlePlugin"));
                    mavenZip.setGroupId(zipGroup);
                    mavenZip.setArtifactId(zipArtifact);
                    mavenZip.setVersion(zipVersion);
                });
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
        this.project = project;
        project.afterEvaluate(evaluatedProject -> { configMaven(project); });
        project.getGradle().getTaskGraph().whenReady(graph -> {
            if (graph.hasTask(LOCALMAVEN)) {
                project.getTasks().getByName(PLUGIN_ZIP_PUBLISH_POM_TASK).setEnabled(false);
            }

        });
    }
}
