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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.gradle.test.rest;

import org.opensearch.gradle.VersionProperties;
import org.opensearch.gradle.info.BuildParams;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import java.util.HashMap;

/**
 * <p>
 * Gradle plugin to help configure {@link CopyRestApiTask}'s and {@link CopyRestTestsTask} that copies the artifacts needed for the Rest API
 * spec and YAML based rest tests.
 * </p>
 * <strong>Rest API specification:</strong> <br>
 * When the {@link RestResourcesPlugin} has been applied the {@link CopyRestApiTask} will automatically copy the core Rest API specification
 * if there are any Rest YAML tests present in source, or copied from {@link CopyRestTestsTask} output.
 * It is recommended (but not required) to also explicitly declare which core specs your project depends on to help optimize the caching
 * behavior.
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restApi {
 *     includeCore 'index', 'cat'
 *   }
 * }
 * </pre>
 * <br>
 * <strong>Rest YAML tests :</strong> <br>
 * When the {@link RestResourcesPlugin} has been applied the {@link CopyRestTestsTask} will copy the Rest YAML tests if explicitly
 * configured with `includeCore` through the `restResources.restTests` extension.
 * <p>
 * Additionally you can specify which sourceSetName resources should be copied to. The default is the yamlRestTest source set.
 * @see CopyRestApiTask
 * @see CopyRestTestsTask
 */
public class RestResourcesPlugin implements Plugin<Project> {

    private static final String EXTENSION_NAME = "restResources";

    @Override
    public void apply(Project project) {
        RestResourcesExtension extension = project.getExtensions().create(EXTENSION_NAME, RestResourcesExtension.class);

        // tests
        Configuration testConfig = project.getConfigurations().create("restTestConfig");
        project.getConfigurations().create("restTests");

        if (BuildParams.isInternal()) {
            // core
            Dependency restTestdependency = project.getDependencies().project(new HashMap<String, String>() {
                {
                    put("path", ":rest-api-spec");
                    put("configuration", "restTests");
                }
            });
            testConfig.withDependencies(s -> s.add(restTestdependency));
        } else {
            Dependency dependency = project.getDependencies().create("org.opensearch:rest-api-spec:" + VersionProperties.getOpenSearch());
            testConfig.withDependencies(s -> s.add(dependency));
        }

        Provider<CopyRestTestsTask> copyRestYamlTestTask = project.getTasks()
            .register("copyYamlTestsTask", CopyRestTestsTask.class, task -> {
                task.includeCore.set(extension.restTests.getIncludeCore());
                task.coreConfig = testConfig;
                task.sourceSetName = SourceSet.TEST_SOURCE_SET_NAME;
                task.dependsOn(task.coreConfig);
            });

        // api
        Configuration specConfig = project.getConfigurations().create("restSpec"); // name chosen for passivity
        project.getConfigurations().create("restSpecs");

        if (BuildParams.isInternal()) {
            Dependency restSpecDependency = project.getDependencies().project(new HashMap<String, String>() {
                {
                    put("path", ":rest-api-spec");
                    put("configuration", "restSpecs");
                }
            });
            specConfig.withDependencies(s -> s.add(restSpecDependency));
        } else {
            Dependency dependency = project.getDependencies().create("org.opensearch:rest-api-spec:" + VersionProperties.getOpenSearch());
            specConfig.withDependencies(s -> s.add(dependency));
        }

        Provider<CopyRestApiTask> copyRestYamlSpecTask = project.getTasks()
            .register("copyRestApiSpecsTask", CopyRestApiTask.class, task -> {
                task.includeCore.set(extension.restApi.getIncludeCore());
                task.dependsOn(copyRestYamlTestTask);
                task.coreConfig = specConfig;
                task.sourceSetName = SourceSet.TEST_SOURCE_SET_NAME;
                task.dependsOn(task.coreConfig);
            });

        project.afterEvaluate(p -> {
            SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
            SourceSet testSourceSet = sourceSets.findByName(SourceSet.TEST_SOURCE_SET_NAME);
            if (testSourceSet != null) {
                project.getTasks().named(testSourceSet.getProcessResourcesTaskName()).configure(t -> t.dependsOn(copyRestYamlSpecTask));
            }
        });
    }
}
