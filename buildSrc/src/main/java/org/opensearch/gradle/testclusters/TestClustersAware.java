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

package org.opensearch.gradle.testclusters;

import org.opensearch.gradle.OpenSearchDistribution;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Nested;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public interface TestClustersAware extends Task {

    @Nested
    Collection<OpenSearchCluster> getClusters();

    @Deprecated(forRemoval = true)
    default void useCluster(OpenSearchCluster cluster) {
        useCluster(getProject(), cluster);
    }

    default void useCluster(Project project, OpenSearchCluster cluster) {
        if (cluster.getPath().equals(project.getPath()) == false) {
            throw new TestClustersException("Task " + getPath() + " can't use test cluster from" + " another project " + cluster);
        }

        // Add configured distributions, plugins and modules as task dependencies so they are built before starting the
        // cluster. Resolved when the task graph is built rather than now, so that using a cluster does not create its
        // nodes while the build script may still be configuring how many there are and how they are numbered.
        dependsOn(
            (Callable<List<Configuration>>) () -> cluster.getNodes()
                .stream()
                .flatMap(node -> node.getDistributions().stream())
                .map(OpenSearchDistribution::getExtracted)
                .collect(Collectors.toList())
        );
        dependsOn(
            (Callable<List<Configuration>>) () -> cluster.getNodes()
                .stream()
                .flatMap(node -> node.getPluginAndModuleConfigurations().stream())
                .collect(Collectors.toList())
        );
        getClusters().add(cluster);
    }

    default void beforeStart() {}

}
