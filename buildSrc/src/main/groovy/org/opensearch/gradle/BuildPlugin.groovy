/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
package org.opensearch.gradle

import groovy.transform.CompileStatic
import org.apache.commons.io.IOUtils
import org.opensearch.gradle.info.BuildParams
import org.opensearch.gradle.info.GlobalBuildInfoPlugin
import org.opensearch.gradle.precommit.PrecommitTasks
import org.opensearch.gradle.test.ErrorReportingTestListener
import org.opensearch.gradle.testclusters.OpenSearchCluster
import org.opensearch.gradle.testclusters.TestClustersPlugin
import org.opensearch.gradle.testclusters.TestDistribution
import org.opensearch.gradle.util.GradleUtils
import org.gradle.api.JavaVersion
import org.gradle.api.*
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.artifacts.repositories.ExclusiveContentRepository
import org.gradle.api.artifacts.repositories.IvyArtifactRepository
import org.gradle.api.artifacts.repositories.IvyPatternRepositoryLayout
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.credentials.HttpHeaderCredentials
import org.gradle.api.execution.TaskActionListener
import org.opensearch.gradle.info.GlobalBuildInfoPlugin
import org.opensearch.gradle.precommit.PrecommitTasks
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.CopySpec
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.tasks.bundling.Jar

/**
 * Encapsulates build configuration for opensearch projects.
 */
@CompileStatic
class BuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.rootProject.pluginManager.apply(GlobalBuildInfoPlugin)

        if (project.pluginManager.hasPlugin('opensearch.standalone-rest-test')) {
            throw new InvalidUserDataException('opensearch.standalone-test, '
                    + 'opensearch.standalone-rest-test, and opensearch.build '
                    + 'are mutually exclusive')
        }
        project.pluginManager.apply('opensearch.java')
        configureLicenseAndNotice(project)
        project.pluginManager.apply('opensearch.publish')
        project.pluginManager.apply(DependenciesInfoPlugin)
        project.pluginManager.apply('jacoco')

        PrecommitTasks.create(project, true)
    }

    static void configureLicenseAndNotice(Project project) {
        ExtraPropertiesExtension ext = project.extensions.getByType(ExtraPropertiesExtension)
        ext.set('licenseFile',  null)
        ext.set('noticeFile', null)
        // add license/notice files
        project.afterEvaluate {
            project.tasks.withType(Jar).configureEach { Jar jarTask ->
                if (ext.has('licenseFile') == false || ext.get('licenseFile') == null || ext.has('noticeFile') == false || ext.get('noticeFile') == null) {
                    throw new GradleException("Must specify license and notice file for project ${project.path}")
                }

                File licenseFile = ext.get('licenseFile') as File
                File noticeFile = ext.get('noticeFile') as File

                jarTask.metaInf { CopySpec spec ->
                    spec.from(licenseFile.parent) { CopySpec from ->
                        from.include licenseFile.name
                        from.rename { 'LICENSE.txt' }
                    }
                    spec.from(noticeFile.parent) { CopySpec from ->
                        from.include noticeFile.name
                        from.rename { 'NOTICE.txt' }
                    }
                }
            }
        }
    }
}
