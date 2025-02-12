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

package org.opensearch.gradle.precommit;

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis;
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin;

import groovy.lang.Closure;

import org.opensearch.gradle.ExportOpenSearchBuildResourcesTask;
import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class ForbiddenApisPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPluginManager().apply(ForbiddenApisPlugin.class);

        TaskProvider<ExportOpenSearchBuildResourcesTask> resourcesTask = project.getTasks()
            .register("forbiddenApisResources", ExportOpenSearchBuildResourcesTask.class);
        Path resourcesDir = project.getBuildDir().toPath().resolve("forbidden-apis-config");
        resourcesTask.configure(t -> {
            t.setOutputDir(resourcesDir.toFile());
            t.copy("forbidden/jdk-signatures.txt");
            t.copy("forbidden/opensearch-all-signatures.txt");
            t.copy("forbidden/opensearch-test-signatures.txt");
            t.copy("forbidden/http-signatures.txt");
            t.copy("forbidden/opensearch-server-signatures.txt");
        });
        project.getTasks().withType(CheckForbiddenApis.class).configureEach(t -> {
            t.dependsOn(resourcesTask);

            assert t.getName().startsWith(ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME);
            String sourceSetName;
            if (ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME.equals(t.getName())) {
                sourceSetName = "main";
            } else {
                // parse out the sourceSetName
                char[] chars = t.getName().substring(ForbiddenApisPlugin.FORBIDDEN_APIS_TASK_NAME.length()).toCharArray();
                chars[0] = Character.toLowerCase(chars[0]);
                sourceSetName = new String(chars);
            }

            SourceSetContainer sourceSets = GradleUtils.getJavaSourceSets(project);
            SourceSet sourceSet = sourceSets.getByName(sourceSetName);
            t.setClasspath(project.files(sourceSet.getRuntimeClasspath()).plus(sourceSet.getCompileClasspath()));

            t.setTargetCompatibility(BuildParams.getRuntimeJavaVersion().getMajorVersion());
            if (BuildParams.getRuntimeJavaVersion().compareTo(JavaVersion.VERSION_14) > 0) {
                // TODO: forbidden apis does not yet support java 15, rethink using runtime version
                t.setTargetCompatibility(JavaVersion.VERSION_14.getMajorVersion());
            }
            t.setBundledSignatures(new HashSet<>(Arrays.asList("jdk-unsafe", "jdk-deprecated", "jdk-non-portable", "jdk-system-out")));
            t.setSignaturesFiles(
                project.files(
                    resourcesDir.resolve("forbidden/jdk-signatures.txt"),
                    resourcesDir.resolve("forbidden/opensearch-all-signatures.txt")
                )
            );
            t.setSuppressAnnotations(new HashSet<>(Arrays.asList("**.SuppressForbidden")));
            if (t.getName().endsWith("Test")) {
                t.setSignaturesFiles(
                    t.getSignaturesFiles()
                        .plus(
                            project.files(
                                resourcesDir.resolve("forbidden/opensearch-test-signatures.txt"),
                                resourcesDir.resolve("forbidden/http-signatures.txt")
                            )
                        )
                );
            } else {
                t.setSignaturesFiles(
                    t.getSignaturesFiles().plus(project.files(resourcesDir.resolve("forbidden/opensearch-server-signatures.txt")))
                );
            }
            ExtraPropertiesExtension ext = t.getExtensions().getExtraProperties();
            ext.set("replaceSignatureFiles", new Closure<Void>(t) {
                @Override
                public Void call(Object... names) {
                    List<Path> resources = new ArrayList<>(names.length);
                    for (Object name : names) {
                        resources.add(resourcesDir.resolve("forbidden/" + name + ".txt"));
                    }
                    t.setSignaturesFiles(project.files(resources));
                    return null;
                }

            });
            ext.set("addSignatureFiles", new Closure<Void>(t) {
                @Override
                public Void call(Object... names) {
                    List<Path> resources = new ArrayList<>(names.length);
                    for (Object name : names) {
                        resources.add(resourcesDir.resolve("forbidden/" + name + ".txt"));
                    }
                    t.setSignaturesFiles(t.getSignaturesFiles().plus(project.files(resources)));
                    return null;
                }
            });
        });
        TaskProvider<Task> forbiddenApis = project.getTasks().named("forbiddenApis");
        forbiddenApis.configure(t -> t.setGroup(""));
        return forbiddenApis;
    }
}
