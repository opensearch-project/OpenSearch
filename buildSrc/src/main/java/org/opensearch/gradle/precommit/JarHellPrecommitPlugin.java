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

import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.util.Util;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

public class JarHellPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        Configuration jarHellConfig = project.getConfigurations().create("jarHell");
        if (BuildParams.isInternal() && project.getPath().equals(":libs:opensearch-common") == false) {
            // External plugins will depend on this already via transitive dependencies.
            // Internal projects are not all plugins, so make sure the check is available
            // we are not doing this for this project itself to avoid jar hell with itself
            project.getDependencies().add("jarHell", project.project(":libs:opensearch-common"));
        }

        TaskProvider<JarHellTask> jarHell = project.getTasks().register("jarHell", JarHellTask.class);
        jarHell.configure(t -> {
            final Task testClassesTask = project.getTasks().getByName("testClasses");
            final SourceSet testSourceSet = Util.getJavaTestSourceSet(project).get();
            t.setClasspath(testSourceSet.getRuntimeClasspath().plus(jarHellConfig));
            t.dependsOn(jarHellConfig, testClassesTask);

            // if this a plugin, we need to add dependency on pluginProperties task
            project.getPluginManager().withPlugin("opensearch.opensearchplugin", plugin -> {
                Task propertiesTask = project.getTasks().getByName("pluginProperties");
                t.dependsOn(jarHellConfig, testClassesTask, propertiesTask);
            });
        });

        return jarHell;
    }
}
