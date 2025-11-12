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

package org.opensearch.gradle.fixtures


import org.opensearch.gradle.Architecture
import org.opensearch.gradle.OpenSearchDistribution
import org.opensearch.gradle.Version
import org.opensearch.gradle.VersionProperties
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner

class DistributionDownloadFixture {

    public static final String INIT_SCRIPT = "repositories-init.gradle"

    static BuildResult withMockedDistributionDownload(GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure) {
        return withMockedDistributionDownload(VersionProperties.getOpenSearch(), OpenSearchDistribution.CURRENT_PLATFORM,
                gradleRunner, buildRunClosure)
    }

    static BuildResult withMockedDistributionDownload(String version, OpenSearchDistribution.Platform platform,
                                                      GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure) {
        String urlPath = urlPath(version, platform);
        return WiremockFixture.withWireMock(urlPath, filebytes(urlPath)) { server ->
            File initFile = new File(gradleRunner.getProjectDir(), INIT_SCRIPT)
            initFile.text = """allprojects { p ->
                p.repositories.all { repo ->
                    repo.allowInsecureProtocol = true
                    repo.setUrl('${server.baseUrl()}')
                }
            }"""
            List<String> givenArguments = gradleRunner.getArguments()
            GradleRunner effectiveRunner = gradleRunner.withArguments(givenArguments + ['-I', initFile.getAbsolutePath()])
            buildRunClosure.delegate = effectiveRunner
            return buildRunClosure.call(effectiveRunner)
        }
    }

    private static String urlPath(String version, OpenSearchDistribution.Platform platform) {
        String fileType = ((platform == OpenSearchDistribution.Platform.LINUX ||
                platform == OpenSearchDistribution.Platform.DARWIN)) ? "tar.gz" : "zip"
        String arch = Architecture.current().name().toLowerCase()
        if (Version.fromString(version).onOrAfter(Version.fromString("1.0.0"))) {
            if (version.contains("SNAPSHOT")) {
                return "/snapshots/core/opensearch/${version}/opensearch-min-${version}-${platform}-${arch}-latest.$fileType"
            }
            return "/releases/core/opensearch/${version}/opensearch-min-${version}-${platform}-${arch}.$fileType"
        } else {
            return "/downloads/elasticsearch/elasticsearch-oss-${version}-${platform}-x86_64.$fileType"
        }
    }

    private static byte[] filebytes(String urlPath) throws IOException {
        String suffix = urlPath.endsWith("zip") ? "zip" : "tar.gz";
        return DistributionDownloadFixture.getResourceAsStream("/org/opensearch/gradle/fake_opensearch." + suffix).getBytes()
    }
}
