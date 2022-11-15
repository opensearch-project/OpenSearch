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

import org.opensearch.gradle.ExportOpenSearchBuildResourcesTask;
import org.opensearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.opensearch.gradle.info.BuildParams;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.TaskProvider;

import java.nio.file.Path;

public class ThirdPartyAuditPrecommitPlugin extends PrecommitPlugin {

    public static final String JDK_JAR_HELL_CONFIG_NAME = "jdkJarHell";
    public static final String LIBS_OPENSEARCH_CORE_PROJECT_PATH = ":libs:opensearch-core";

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        project.getConfigurations().create("forbiddenApisCliJar");
        project.getDependencies().add("forbiddenApisCliJar", "de.thetaphi:forbiddenapis:3.4");

        Configuration jdkJarHellConfig = project.getConfigurations().create(JDK_JAR_HELL_CONFIG_NAME);
        if (BuildParams.isInternal() && project.getPath().equals(":libs:opensearch-core") == false) {
            // External plugins will depend on this already via transitive dependencies.
            // Internal projects are not all plugins, so make sure the check is available
            // we are not doing this for this project itself to avoid jar hell with itself
            project.getDependencies().add(JDK_JAR_HELL_CONFIG_NAME, project.project(LIBS_OPENSEARCH_CORE_PROJECT_PATH));
        }

        TaskProvider<ExportOpenSearchBuildResourcesTask> resourcesTask = project.getTasks()
            .register("thirdPartyAuditResources", ExportOpenSearchBuildResourcesTask.class);
        Path resourcesDir = project.getBuildDir().toPath().resolve("third-party-audit-config");
        resourcesTask.configure(t -> {
            t.setOutputDir(resourcesDir.toFile());
            t.copy("forbidden/third-party-audit.txt");
        });
        TaskProvider<ThirdPartyAuditTask> audit = project.getTasks().register("thirdPartyAudit", ThirdPartyAuditTask.class);
        audit.configure(t -> {
            t.dependsOn(resourcesTask);
            t.setJavaHome(BuildParams.getRuntimeJavaHome().toString());
            t.getTargetCompatibility().set(project.provider(BuildParams::getRuntimeJavaVersion));
            t.setSignatureFile(resourcesDir.resolve("forbidden/third-party-audit.txt").toFile());
        });
        project.getTasks().withType(ThirdPartyAuditTask.class).configureEach(t -> t.setJdkJarHellClasspath(jdkJarHellConfig));
        return audit;
    }
}
