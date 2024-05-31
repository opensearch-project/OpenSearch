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
    private final static String DEFAULT_GROUP_ID = "org.opensearch.plugin";

    public final static String PUBLICATION_NAME = "pluginZip";
    public final static String STAGING_REPO = "zipStaging";
    public final static String LOCAL_STAGING_REPO_PATH = "/build/local-staging-repo";
    // TODO: Does the path ^^ need to use platform dependant file separators ?

    /**
     * This method returns a "default" groupId value ("{@link #DEFAULT_GROUP_ID}").
     * It is possible to have the `group` property unspecified in which case the default value is used instead.
     * See <a href="https://github.com/opensearch-project/OpenSearch/pull/4156#issuecomment-1230397082">GitHub discussion</a>
     * for details.
     *
     * @deprecated This method will be removed in OpenSearch 3.x and `group` property will be required
     * @return The default groupId value
     */
    @Deprecated
    public static String getDefaultGroupId(Project project) {
        project.getLogger()
            .warn(
                String.format(
                    "The 'project.group' property is empty, a default value '%s' will be used instead. "
                        + "Please notice that in OpenSearch 3.x the 'project.group' property will be required.",
                    DEFAULT_GROUP_ID
                )
            );
        return DEFAULT_GROUP_ID;
    }

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
                    if (mavenZip.getGroupId().isEmpty()) {
                        mavenZip.setGroupId(getDefaultGroupId(project));
                    }
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

                // There are number of tasks prefixed by 'publishPluginZipPublication', f.e.:
                // publishPluginZipPublicationToZipStagingRepository, publishPluginZipPublicationToMavenLocal
                final Set<Task> publishPluginZipPublicationToTasks = project.getTasks()
                    .stream()
                    .filter(t -> t.getName().startsWith("publishPluginZipPublicationTo"))
                    .collect(Collectors.toSet());
                if (!publishPluginZipPublicationToTasks.isEmpty()) {
                    if (validatePluginZipPom != null) {
                        publishPluginZipPublicationToTasks.forEach(t -> t.dependsOn(validatePluginZipPom));
                    } else {
                        publishPluginZipPublicationToTasks.forEach(t -> t.dependsOn("generatePomFileForNebulaPublication"));
                    }
                }
            } else {
                project.getLogger()
                    .warn(String.format("Plugin 'opensearch.pluginzip' is applied but no '%s' publication is defined.", PUBLICATION_NAME));
            }
        });
    }
}
