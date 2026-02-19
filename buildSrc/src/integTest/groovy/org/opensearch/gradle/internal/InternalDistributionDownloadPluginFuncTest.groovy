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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.gradle.internal

import org.opensearch.gradle.Architecture
import org.opensearch.gradle.VersionProperties
import org.opensearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder

import java.lang.management.ManagementFactory

class InternalDistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    def "plugin application fails on non internal build"() {
        given:
        buildFile.text = """
            plugins {
             id 'opensearch.internal-distribution-download'
            }
        """

        when:
        def result = gradleRunner("tasks").buildAndFail()

        then:
        assertOutputContains(result.output, "Plugin 'opensearch.internal-distribution-download' is not supported. " +
            "Use 'opensearch.distribution-download' plugin instead")
    }

    def "resolves current version from local build"() {
        given:
        internalBuild()
        def archive = archiveTask()
        localDistroSetup(archive)
        def distroVersion = VersionProperties.getOpenSearch()
        buildFile << """
            apply plugin: 'opensearch.internal-distribution-download'

            opensearch_distributions {
              test_distro {
                  version = "$distroVersion"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
              }
            }
            tasks.register("setupDistro", Sync) {
                from(opensearch_distributions.test_distro.extracted)
                into("build/distro")
            }
        """

        when:
        def result = gradleRunner("setupDistro", '-g', testProjectDir.newFolder('GUH').path).build()

        then:
        result.task(":distribution:archives:${archive}:buildExpanded").outcome == TaskOutcome.SUCCESS
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated("build/distro", 'current-marker.txt')
    }

    def "resolves expanded bwc versions from source"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        buildFile << """
            apply plugin: 'opensearch.internal-distribution-download'

            opensearch_distributions {
              test_distro {
                  version = "8.1.0"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
              }
            }
            tasks.register("setupDistro", Sync) {
                from(opensearch_distributions.test_distro.extracted)
                into("build/distro")
            }
        """
        when:

        def result = gradleRunner("setupDistro").build()
        then:
        result.task(":distribution:bwc:minor:buildBwcExpandedTask").outcome == TaskOutcome.SUCCESS
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroIsCreated("distribution/bwc/minor/build/install/opensearch-distro",
                'bwc-marker.txt')
    }

    def "fails on resolving bwc versions with no bundled jdk"() {
        given:
        internalBuild()
        bwcMinorProjectSetup()
        buildFile << """
            import org.opensearch.gradle.JavaPackageType

            apply plugin: 'opensearch.internal-distribution-download'

            opensearch_distributions {
              test_distro {
                  version = "8.1.0"
                  type = "archive"
                  platform = "linux"
                  architecture = Architecture.current();
                  bundledJdk = JavaPackageType.NONE
              }
            }
            tasks.register("createExtractedTestDistro") {
                dependsOn opensearch_distributions.test_distro.extracted
            }
        """
        when:
        def result = gradleRunner("createExtractedTestDistro").buildAndFail()
        then:
        assertOutputContains(result.output, "Configuring a snapshot bwc distribution ('test_distro') " +
                "without a bundled JDK is not supported.")
    }

    private void bwcMinorProjectSetup() {
        settingsFile << """
        include ':distribution:bwc:minor'
        """
        def archive = archiveTask()
        def bwcSubProjectFolder = testProjectDir.newFolder("distribution", "bwc", "minor")
        new File(bwcSubProjectFolder, 'bwc-marker.txt') << "bwc=minor"
        new File(bwcSubProjectFolder, 'build.gradle') << """
            apply plugin:'base'

            // packed distro
            configurations.create("${archive}")
            tasks.register("buildBwcTask", Tar) {
                from('bwc-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            artifacts {
                it.add("${archive}", buildBwcTask)
            }

            // expanded distro
            configurations.create("expanded-${archive}")
            def expandedTask = tasks.register("buildBwcExpandedTask", Copy) {
                from('bwc-marker.txt')
                into('build/install/opensearch-distro')
            }
            artifacts {
                it.add("expanded-${archive}", file('build/install')) {
                    builtBy expandedTask
                    type = 'directory'
                }
            }
        """
    }

    private String archiveTask() {
        return Architecture.current() == Architecture.X64 ? "linux-tar" : "linux-${Architecture.current().name().toLowerCase()}-tar"; 
    }

    private void localDistroSetup(def archive) {
        settingsFile << """
        include ":distribution:archives:${archive}"
        """
        def bwcSubProjectFolder = testProjectDir.newFolder("distribution", "archives", "${archive}")
        new File(bwcSubProjectFolder, 'current-marker.txt') << "current"
        new File(bwcSubProjectFolder, 'build.gradle') << """
            import org.gradle.api.internal.artifacts.ArtifactAttributes;

            apply plugin:'distribution'

            def buildTar = tasks.register("buildTar", Tar) {
                from('current-marker.txt')
                archiveExtension = "tar.gz"
                compression = Compression.GZIP
            }
            def buildExpanded = tasks.register("buildExpanded", Copy) {
                from('current-marker.txt')
                into("build/local")
            }
            configurations {
                extracted {
                    attributes {
                          attribute(ArtifactAttributes.ARTIFACT_FORMAT, "directory")
                    }
                }
            }
            artifacts {
                it.add("default", buildTar)
                it.add("extracted", buildExpanded)
            }
        """
        buildFile << """
        """
    }

    boolean assertExtractedDistroIsCreated(String relativeDistroPath, String markerFileName) {
        File extractedFolder = new File(testProjectDir.root, relativeDistroPath)
        assert extractedFolder.exists()
        assert new File(extractedFolder, markerFileName).exists()
        true
    }
}
