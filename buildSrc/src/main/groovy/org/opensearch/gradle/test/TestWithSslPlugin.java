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

package org.opensearch.gradle.test;

import org.opensearch.gradle.ExportOpenSearchBuildResourcesTask;
import org.opensearch.gradle.precommit.ForbiddenPatternsTask;
import org.opensearch.gradle.testclusters.OpenSearchCluster;
import org.opensearch.gradle.testclusters.TestClustersAware;
import org.opensearch.gradle.testclusters.TestClustersPlugin;
import org.opensearch.gradle.util.Util;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;

public class TestWithSslPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        File keyStoreDir = new File(project.getLayout().getBuildDirectory().getAsFile().get(), "keystore");
        TaskProvider<ExportOpenSearchBuildResourcesTask> exportKeyStore = project.getTasks()
            .register("copyTestCertificates", ExportOpenSearchBuildResourcesTask.class, (t) -> {
                t.copy("test/ssl/test-client.crt");
                t.copy("test/ssl/test-client.jks");
                t.copy("test/ssl/test-node.crt");
                t.copy("test/ssl/test-node.jks");
                t.setOutputDir(keyStoreDir);
            });

        project.getPlugins().withType(StandaloneRestTestPlugin.class).configureEach(restTestPlugin -> {
            SourceSet testSourceSet = Util.getJavaTestSourceSet(project).get();
            testSourceSet.getResources().srcDir(new File(keyStoreDir, "test/ssl"));
            testSourceSet.compiledBy(exportKeyStore);

            project.getTasks().withType(TestClustersAware.class).configureEach(clusterAware -> clusterAware.dependsOn(exportKeyStore));

            // Tell the tests we're running with ssl enabled
            project.getTasks()
                .withType(RestIntegTestTask.class)
                .configureEach(runner -> runner.systemProperty("tests.ssl.enabled", "true"));
        });

        project.getPlugins().withType(TestClustersPlugin.class).configureEach(clustersPlugin -> {
            File keystoreDir = new File(project.getLayout().getBuildDirectory().getAsFile().get(), "keystore/test/ssl");
            File nodeKeystore = new File(keystoreDir, "test-node.jks");
            File clientKeyStore = new File(keystoreDir, "test-client.jks");
            @SuppressWarnings("unchecked")
            NamedDomainObjectContainer<OpenSearchCluster> clusters = (NamedDomainObjectContainer<OpenSearchCluster>) project.getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            clusters.all(c -> {
                // copy keystores & certs into config/
                c.extraConfigFile(nodeKeystore.getName(), nodeKeystore);
                c.extraConfigFile(clientKeyStore.getName(), clientKeyStore);
            });
        });

        project.getTasks()
            .withType(ForbiddenPatternsTask.class)
            .configureEach(forbiddenPatternTask -> forbiddenPatternTask.exclude("**/*.crt"));
    }
}
