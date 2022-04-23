/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gradle.zipplugin;

import java.util.*;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import java.nio.file.Path;
import org.gradle.api.Task;

public class ZipPublish implements Plugin<Project> {
    private Project project;

    public final static String EXTENSION_NAME = "zipmavensettings";
    public final static String PUBLICATION_NAME = "mavenzip";
    public final static String STAGING_REPO = "zipstaging";
    public final static String MAVEN_ZIP_PUBLISH_POM_TASK = "generatePomFileForMavenzipPublication";
    public final static String LOCALMAVEN = "publishToMavenLocal";
    public final static String LOCAL_STAGING_REPO_PATH = "/build/local-staging-repo";
    public String zipDistributionLocation = "/build/distributions/";

    private void configMaven(Project project) {
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
                    ZipPublishExtension extset = project.getExtensions().findByType(ZipPublishExtension.class);
                    // Getting the Zip group from created extension
                    String zipGroup = extset.getZipGroup();
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
        project.getExtensions().create(EXTENSION_NAME, ZipPublishExtension.class);
        project.afterEvaluate(evaluatedProject -> { configMaven(project); });
        Task compileJava = project.getTasks().findByName("compileJava");
        if (compileJava != null) {
            compileJava.setEnabled(false);
        }
        Task sourceJarTask = project.getTasks().findByName("sourcesJar");
        if (sourceJarTask != null) {
            sourceJarTask.setEnabled(false);
        }
        Task javaDocJarTask = project.getTasks().findByName("javadocJar");
        if (javaDocJarTask != null) {
            javaDocJarTask.setEnabled(false);
        }
        project.getGradle().getTaskGraph().whenReady(graph -> {
            if (graph.hasTask(LOCALMAVEN)) {
                project.getTasks().getByName(MAVEN_ZIP_PUBLISH_POM_TASK).setEnabled(false);
            }

        });
    }
}
