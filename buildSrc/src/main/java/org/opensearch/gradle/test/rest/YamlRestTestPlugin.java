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

package org.opensearch.gradle.test.rest;

import org.opensearch.gradle.OpenSearchJavaPlugin;
import org.opensearch.gradle.test.RestIntegTestTask;
import org.opensearch.gradle.test.RestTestBasePlugin;
import org.opensearch.gradle.testclusters.TestClustersPlugin;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

/**
 * Apply this plugin to run the YAML based REST tests.
 */
public class YamlRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "yamlRestTest";

    @Override
    public void apply(Project project) {

        project.getPluginManager().apply(OpenSearchJavaPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(RestResourcesPlugin.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlTestSourceSet = sourceSets.create(SOURCE_SET_NAME);

        // create the test cluster container
        RestTestUtil.createTestCluster(project, yamlTestSourceSet);

        // setup the yamlRestTest task
        Provider<RestIntegTestTask> yamlRestTestTask = RestTestUtil.registerTask(project, yamlTestSourceSet);

        // setup the dependencies
        RestTestUtil.setupDependencies(project, yamlTestSourceSet);

        // setup the copy for the rest resources
        project.getTasks().withType(CopyRestApiTask.class, copyRestApiTask -> {
            copyRestApiTask.sourceSetName = SOURCE_SET_NAME;
            project.getTasks().named(yamlTestSourceSet.getProcessResourcesTaskName()).configure(t -> t.dependsOn(copyRestApiTask));
        });
        project.getTasks().withType(CopyRestTestsTask.class, copyRestTestTask -> { copyRestTestTask.sourceSetName = SOURCE_SET_NAME; });

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, yamlTestSourceSet);

        // wire this task into check
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(yamlRestTestTask));
    }
}
