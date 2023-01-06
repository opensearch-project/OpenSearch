/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gradle;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import com.github.jengelman.gradle.plugins.shadow.ShadowExtension;
import groovy.util.Node;
import groovy.util.NodeList;

import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.precommit.PomValidationPrecommitPlugin;
import org.opensearch.gradle.util.Util;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.XmlProvider;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.BasePluginConvention;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.Callable;

import static org.opensearch.gradle.util.GradleUtils.maybeConfigure;

public class PublishPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply("com.netflix.nebula.maven-base-publish");
        project.getPluginManager().apply("com.netflix.nebula.maven-nebula-publish");
        project.getPluginManager().apply(PomValidationPrecommitPlugin.class);

        configureJavadocJar(project);
        configureSourcesJar(project);
        configurePomGeneration(project);
    }

    private static String getArchivesBaseName(Project project) {
        return project.getConvention().getPlugin(BasePluginConvention.class).getArchivesBaseName();
    }

    /**Configuration generation of maven poms. */
    private static void configurePomGeneration(Project project) {

        TaskProvider<Task> generatePomTask = project.getTasks().register("generatePom");

        maybeConfigure(project.getTasks(), LifecycleBasePlugin.ASSEMBLE_TASK_NAME, assemble -> assemble.dependsOn(generatePomTask));

        project.getTasks().withType(GenerateMavenPom.class).configureEach(pomTask -> pomTask.setDestination(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return String.format(
                    "%s/distributions/%s-%s.pom",
                    project.getBuildDir(),
                    pomTask.getName().toLowerCase().contains("zip") ? project.getName() : getArchivesBaseName(project),
                    project.getVersion()
                );
            }
        }));

        PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);

        project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", plugin -> {
            MavenPublication publication = publishing.getPublications().maybeCreate("shadow", MavenPublication.class);
            ShadowExtension shadow = project.getExtensions().getByType(ShadowExtension.class);
            shadow.component(publication);
            // Workaround for https://github.com/johnrengelman/shadow/issues/334
            // Here we manually add any project dependencies in the "shadow" configuration to our generated POM
            publication.getPom().withXml(xml -> {
                Node root = xml.asNode();
                if (((NodeList) root.get("name")).isEmpty()) {
                    root.appendNode("name", project.getName());
                }
                if (((NodeList) root.get("description")).isEmpty()) {
                    root.appendNode("description", project.getDescription());
                }
                Node dependenciesNode = (Node) ((NodeList) root.get("dependencies")).get(0);
                project.getConfigurations().getByName(ShadowBasePlugin.CONFIGURATION_NAME).getAllDependencies().all(dependency -> {
                    if (dependency instanceof ProjectDependency) {
                        Node dependencyNode = dependenciesNode.appendNode("dependency");
                        dependencyNode.appendNode("groupId", dependency.getGroup());
                        ProjectDependency projectDependency = (ProjectDependency) dependency;
                        String artifactId = getArchivesBaseName(projectDependency.getDependencyProject());
                        dependencyNode.appendNode("artifactId", artifactId);
                        dependencyNode.appendNode("version", dependency.getVersion());
                        dependencyNode.appendNode("scope", "compile");
                    }
                });
            });
        });

        publishing.getPublications().withType(MavenPublication.class, publication -> {
            // Add git origin info to generated POM files
            publication.getPom().withXml(PublishPlugin::addScmInfo);

            if (!publication.getName().toLowerCase().contains("zip")) {
                // have to defer this until archivesBaseName is set
                project.afterEvaluate(p -> publication.setArtifactId(getArchivesBaseName(project)));

                // publish sources and javadoc for Java projects.
                if (project.getPluginManager().hasPlugin("opensearch.java")) {
                    publication.artifact(project.getTasks().getByName("sourcesJar"));
                    publication.artifact(project.getTasks().getByName("javadocJar"));
                }
            } else {
                project.afterEvaluate(p -> publication.setArtifactId(project.getName()));
            }

            generatePomTask.configure(
                t -> t.dependsOn(String.format("generatePomFileFor%sPublication", Util.capitalize(publication.getName())))
            );
        });

    }

    private static void addScmInfo(XmlProvider xml) {
        Node root = xml.asNode();
        Node url = null, scm = null;

        for (final Object child : root.children()) {
            if (child instanceof Node) {
                final Node node = (Node) child;
                final Object name = node.name();

                try {
                    // For Gradle 6.8 and below, the class is groovy.xml.QName
                    // For Gradle 7.4 and above, the class is groovy.namespace.QName
                    if (name != null && name.getClass().getSimpleName().equals("QName")) {
                        final MethodHandle handle = MethodHandles.publicLookup()
                            .findVirtual(name.getClass(), "matches", MethodType.methodType(boolean.class, Object.class))
                            .bindTo(name);

                        if ((boolean) handle.invoke("url")) {
                            url = node;
                        } else if ((boolean) handle.invoke("scm")) {
                            scm = node;
                        }
                    }
                } catch (final Throwable ex) {
                    // Not a suitable QName type we could use ...
                }

                if ("url".equals(name)) {
                    url = node;
                } else if ("scm".equals(name)) {
                    scm = node;
                }
            }
        }

        // Only include URL section if it is not provided in the POM already
        if (url == null) {
            root.appendNode("url", Util.urlFromOrigin(BuildParams.getGitOrigin()));
        }

        // Only include SCM section if it is not provided in the POM already
        if (scm == null) {
            Node scmNode = root.appendNode("scm");
            scmNode.appendNode("url", BuildParams.getGitOrigin());
        }
    }

    /** Adds a javadocJar task to generate a jar containing javadocs. */
    private static void configureJavadocJar(Project project) {
        project.getPlugins().withId("opensearch.java", p -> {
            TaskProvider<Jar> javadocJarTask = project.getTasks().register("javadocJar", Jar.class);
            javadocJarTask.configure(jar -> {
                jar.getArchiveClassifier().set("javadoc");
                jar.setGroup("build");
                jar.setDescription("Assembles a jar containing javadocs.");
                jar.from(project.getTasks().named(JavaPlugin.JAVADOC_TASK_NAME));
            });
            maybeConfigure(project.getTasks(), BasePlugin.ASSEMBLE_TASK_NAME, t -> t.dependsOn(javadocJarTask));
        });
    }

    static void configureSourcesJar(Project project) {
        project.getPlugins().withId("opensearch.java", p -> {
            TaskProvider<Jar> sourcesJarTask = project.getTasks().register("sourcesJar", Jar.class);
            sourcesJarTask.configure(jar -> {
                jar.getArchiveClassifier().set("sources");
                jar.setGroup("build");
                jar.setDescription("Assembles a jar containing source files.");
                SourceSet mainSourceSet = Util.getJavaMainSourceSet(project).get();
                jar.from(mainSourceSet.getAllSource());
            });
            maybeConfigure(project.getTasks(), BasePlugin.ASSEMBLE_TASK_NAME, t -> t.dependsOn(sourcesJarTask));
        });
    }
}
