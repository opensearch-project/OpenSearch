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

package org.opensearch.gradle


import org.opensearch.gradle.Architecture
import org.opensearch.gradle.fixtures.AbstractGradleFuncTest
import org.opensearch.gradle.transform.SymbolicLinkPreservingUntarTransform
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

import static org.opensearch.gradle.fixtures.DistributionDownloadFixture.withMockedDistributionDownload

class DistributionDownloadPluginFuncTest extends AbstractGradleFuncTest {

    @Unroll
    def "#distType version can be resolved"() {
        given:
        buildFile << applyPluginAndSetupDistro(version, platform)

        when:
        def result = withMockedDistributionDownload(version, platform, gradleRunner('setupDistro', '-i')) {
            build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        assertExtractedDistroCreated("build/distro")

        where:
        version                              | platform                                   | distType
        VersionProperties.getOpenSearch()    | OpenSearchDistribution.Platform.LINUX      | "current"
        "2.19.0-SNAPSHOT"                    | OpenSearchDistribution.Platform.LINUX   | "bwc"
        "3.3.0"                              | OpenSearchDistribution.Platform.WINDOWS | "released"
    }


    def "transformed versions are kept across builds"() {
        given:
        def version = VersionProperties.getOpenSearch()
        def platform = OpenSearchDistribution.Platform.LINUX
        def arch = Architecture.current().name().toLowerCase()

        buildFile << applyPluginAndSetupDistro(version, platform)
        buildFile << """
            apply plugin:'base'
        """

        when:
        def customGradleUserHome = testProjectDir.newFolder().absolutePath;
        def runner = gradleRunner('clean', 'setupDistro', '-i', '-g', customGradleUserHome)
        def result = withMockedDistributionDownload(version, platform, runner) {
            // initial run
            build()
            // 2nd invocation
            build()
        }

        then:
        result.task(":setupDistro").outcome == TaskOutcome.SUCCESS
        result.output.count("Unpacking opensearch-${version}-linux-${arch}.tar.gz " +
            "using SymbolicLinkPreservingUntarTransform.") == 0
    }

    def "transforms are reused across projects"() {
        given:
        def version = VersionProperties.getOpenSearch()
        def platform = OpenSearchDistribution.Platform.LINUX
        def arch = Architecture.current().name().toLowerCase()

        3.times {
            testProjectDir.newFolder("sub-$it")
            settingsFile << """
                include ':sub-$it'
            """
        }
        buildFile.text = """
            import org.opensearch.gradle.Architecture

            plugins {
                id 'opensearch.distribution-download'
            }

            subprojects {
                apply plugin: 'opensearch.distribution-download'

                ${setupTestDistro(version, platform)}
                ${setupDistroTask()}
            }
        """

        when:
        def customGradleUserHome = testProjectDir.newFolder().absolutePath;
        def runner = gradleRunner('setupDistro', '-i', '-g', customGradleUserHome)
        def result = withMockedDistributionDownload(version, platform, runner) {
            build()
        }

        then:
        result.tasks.size() == 3
        result.output.count("Unpacking opensearch-${version}-linux-${arch}.tar.gz " +
                "using SymbolicLinkPreservingUntarTransform.") == 1
    }

    private boolean assertExtractedDistroCreated(String relativePath) {
        File distroExtracted = new File(testProjectDir.root, relativePath)
        assert distroExtracted.exists()
        assert distroExtracted.isDirectory()
        assert new File(distroExtracted, "opensearch-1.2.3/bin/opensearch").exists()
        true
    }

    private static String applyPluginAndSetupDistro(String version, OpenSearchDistribution.Platform platform) {
        """
            import org.opensearch.gradle.Architecture

            plugins {
                id 'opensearch.distribution-download'
            }

            ${setupTestDistro(version, platform)}
            ${setupDistroTask()}

        """
    }

    private static String setupTestDistro(String version, OpenSearchDistribution.Platform platform) {
        return """
            opensearch_distributions {
                test_distro {
                    version = "$version"
                    type = "archive"
                    platform = "$platform"
                    architecture = Architecture.current();
                }
            }
            """
    }

    private static String setupDistroTask() {
        return """
            tasks.register("setupDistro", Sync) {
                from(opensearch_distributions.test_distro.extracted)
                into("build/distro")
            }
            """
    }
}
