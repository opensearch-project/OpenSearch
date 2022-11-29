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

import org.gradle.internal.os.OperatingSystem
import org.opensearch.gradle.VersionProperties
import org.opensearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

class InternalDistributionArchiveCheckPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        ["darwin-zip", 'darwin-tar'].each { projName ->
            settingsFile << """
            include ':${projName}'
            """

            file("${projName}/build.gradle") << """
                plugins {
                  id 'opensearch.internal-distribution-archive-check'
                }"""
        }
        file("SomeFile.txt") << """
            some dummy txt file
        """

        buildFile << """
            allprojects {
                apply plugin:'base'
                ext.elasticLicenseUrl = "http://foo.bar"
            }
            tasks.register("buildDarwinTar", Tar) {
                compression = Compression.GZIP
                from 'SomeFile.class'
            }
            tasks.register("buildDarwinZip", Zip) {
                from 'SomeFile.txt'
            }"""
    }

    @Unroll
    def "plain class files in distribution #archiveType archives are detected"() {
        given:
        file("SomeFile.class") << """
            some dummy class file
        """
        buildFile << """
            tasks.withType(AbstractArchiveTask).configureEach {
                from 'SomeFile.class'
            }
        """
        if (OperatingSystem.current() == OperatingSystem.WINDOWS) {
            when:
            def result = gradleRunner(":darwin-${archiveType}:check", '--stacktrace').build()
            then:
            result.task(":darwin-${archiveType}:checkExtraction").outcome == TaskOutcome.SKIPPED
        } else {
            when:
            def result = gradleRunner(":darwin-${archiveType}:check", '--stacktrace').buildAndFail()
            then:
            result.task(":darwin-${archiveType}:checkExtraction").outcome == TaskOutcome.FAILED
            result.output.contains("Detected class file in distribution ('SomeFile.class')")
        }

        where:
        archiveType << ["zip", 'tar']
    }

    def "fails on unexpected notice content"() {
        given:
        license(file("LICENSE.txt"))
        file("NOTICE.txt").text = """OpenSearch (https://opensearch.org/)
Copyright 2009-2018 Acme Coorp"""
        buildFile << """
            apply plugin:'base'
            tasks.withType(AbstractArchiveTask).configureEach {
                into("opensearch-${VersionProperties.getOpenSearch()}") {
                    from 'LICENSE.txt'
                    from 'SomeFile.txt'
                    from 'NOTICE.txt'
                }
            }
        """

        if (OperatingSystem.current() == OperatingSystem.WINDOWS) {
            when:
            def result = gradleRunner(":darwin-tar:checkNotice").build()
            then:
            result.task(":darwin-tar:checkNotice").outcome == TaskOutcome.SKIPPED
        } else {
            when:
            def result = gradleRunner(":darwin-tar:checkNotice").buildAndFail()
            then:
            result.task(":darwin-tar:checkNotice").outcome == TaskOutcome.FAILED
            normalizedOutput(result.output).contains("> expected line [2] in " +
                "[./darwin-tar/build/tar-extracted/opensearch-${VersionProperties.getOpenSearch()}/NOTICE.txt] " +
                "to be [Copyright OpenSearch Contributors] but was [Copyright 2009-2018 Acme Coorp]")
        }

    }

    void license(File file = file("licenses/APACHE-LICENSE-2.0.txt")) {
        file << """license coorp stuff line 1
license coorp stuff line 2
license coorp stuff line 3
"""
    }

}
