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

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Nested;

import java.util.Collection;
import java.util.concurrent.Callable;

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

        // Add configured distributions as task dependencies so they are built before starting the cluster
        cluster.getNodes().stream().flatMap(node -> node.getDistributions().stream()).forEach(distro -> dependsOn(distro.getExtracted()));

        cluster.getNodes().forEach(node -> dependsOn((Callable<Collection<Configuration>>) node::getPluginAndModuleConfigurations));
        getClusters().add(cluster);
    }

    default void beforeStart() {}

}
