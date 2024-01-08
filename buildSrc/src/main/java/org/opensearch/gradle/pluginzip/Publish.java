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
import org.gradle.api.Task;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

public class Publish implements Plugin<Project> {

    public final static String PUBLICATION_NAME = "pluginZip";
    public final static String STAGING_REPO = "zipStaging";
    public final static String LOCAL_STAGING_REPO_PATH = "/build/local-staging-repo";
    // TODO: Does the path ^^ need to use platform dependant file separators ?

    private boolean isZipPublicationPresent(Project project) {
        PublishingExtension pe = project.getExtensions().findByType(PublishingExtension.class);
        if (pe == null) {
            return false;
        }
        return pe.getPublications().findByName(PUBLICATION_NAME) != null;
    }

    private void addLocalMavenRepo(Project project) {
        final Path buildDirectory = project.getRootDir().toPath();
        project.getExtensions().configure(PublishingExtension.class, publishing -> {
            publishing.repositories(repositories -> {
                repositories.maven(maven -> {
                    maven.setName(STAGING_REPO);
                    maven.setUrl(buildDirectory.toString() + LOCAL_STAGING_REPO_PATH);
                });
            });
        });
    }

    private void addZipArtifact(Project project) {
        project.getExtensions().configure(PublishingExtension.class, publishing -> {
            publishing.publications(publications -> {
                MavenPublication mavenZip = (MavenPublication) publications.findByName(PUBLICATION_NAME);
                if (mavenZip != null) {
                    mavenZip.artifact(project.getTasks().named("bundlePlugin"));
                }
            });
        });
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply("com.netflix.nebula.maven-nebula-publish");
        project.getPluginManager().apply(MavenPublishPlugin.class);
        project.afterEvaluate(evaluatedProject -> {
            if (isZipPublicationPresent(project)) {
                addLocalMavenRepo(project);
                addZipArtifact(project);
                Task validatePluginZipPom = project.getTasks().findByName("validatePluginZipPom");
                if (validatePluginZipPom != null) {
                    validatePluginZipPom.dependsOn("generatePomFileForNebulaPublication");
                }

                // There are number of tasks prefixed by 'publishPluginZipPublication', f.e.:
                // publishPluginZipPublicationToZipStagingRepository, publishPluginZipPublicationToMavenLocal
                final Set<Task> publishPluginZipPublicationToTasks = project.getTasks()
                    .stream()
                    .filter(t -> t.getName().startsWith("publishPluginZipPublicationTo"))
                    .collect(Collectors.toSet());
                if (!publishPluginZipPublicationToTasks.isEmpty()) {
                    publishPluginZipPublicationToTasks.forEach(t -> t.dependsOn("generatePomFileForNebulaPublication"));
                }
            } else {
                project.getLogger()
                    .warn(String.format("Plugin 'opensearch.pluginzip' is applied but no '%s' publication is defined.", PUBLICATION_NAME));
            }
        });
    }
}
