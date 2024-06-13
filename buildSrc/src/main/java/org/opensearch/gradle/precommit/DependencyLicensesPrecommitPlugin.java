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

import org.opensearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

public class DependencyLicensesPrecommitPlugin extends PrecommitPlugin {

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        TaskProvider<DependencyLicensesTask> dependencyLicenses = project.getTasks()
            .register("dependencyLicenses", DependencyLicensesTask.class);

        final Configuration runtimeClasspath = project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME);
        final Configuration compileOnly = project.getConfigurations()
            .getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
        final Provider<FileCollection> provider = project.provider(
            () -> GradleUtils.getFiles(project, runtimeClasspath, dependency -> dependency instanceof ProjectDependency == false)
                .minus(compileOnly)
        );

        // only require dependency licenses for non-opensearch deps
        dependencyLicenses.configure(t -> t.getDependencies().set(provider));

        // we also create the updateShas helper task that is associated with dependencyLicenses
        project.getTasks().register("updateShas", UpdateShasTask.class, t -> t.setParentTask(dependencyLicenses));
        return dependencyLicenses;
    }
}
