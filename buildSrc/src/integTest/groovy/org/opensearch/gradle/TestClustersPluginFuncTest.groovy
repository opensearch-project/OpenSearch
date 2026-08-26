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

import org.gradle.testkit.runner.GradleRunner
import org.opensearch.gradle.fixtures.AbstractGradleFuncTest
import spock.lang.IgnoreIf

import java.security.MessageDigest

import static org.opensearch.gradle.fixtures.DistributionDownloadFixture.withMockedDistributionDownload

/**
 * We do not have coverage for the test cluster startup on windows yet.
 * One step at a time...
 * */
@IgnoreIf({ os.isWindows() })
class TestClustersPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """
            import org.opensearch.gradle.testclusters.DefaultTestClustersTask
            plugins {
                id 'opensearch.testclusters'
            }

            class SomeClusterAwareTask extends DefaultTestClustersTask {
                @TaskAction void doSomething() {
                    println 'SomeClusterAwareTask executed'
                }
            }

            class RestartingClusterAwareTask extends DefaultTestClustersTask {
                @TaskAction void doSomething() {
                    clusters.each { it.restart() }
                    println 'RestartingClusterAwareTask executed'
                }
            }
        """
    }

    def "test cluster distribution is copied into the working directory and started"() {
        given:
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'archive'
              }
            }

            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """

        when:
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) {
            build()
        }

        then:
        result.output.contains("opensearch-keystore script executed!")
        assertOpenSearchStdoutContains("myCluster", "Starting OpenSearch process")
        assertOpenSearchStdoutContains("myCluster", "Stopping node")
        assertCustomDistro('myCluster')
    }

    def "custom distro folder created for tweaked cluster distribution"() {
        given:
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'archive'
                extraJarFile(file('${someJar().absolutePath}'))
              }
            }

            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """

        when:
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) {
            build()
        }

        then:
        result.output.contains("opensearch-keystore script executed!")
        assertOpenSearchStdoutContains("myCluster", "Starting OpenSearch process")
        assertOpenSearchStdoutContains("myCluster", "Stopping node")
        assertCustomDistro('myCluster')
    }

    def "transform cache remains unchanged across repeated builds"() {
        given:
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'archive'
              }
            }

            tasks.register('myTask', SomeClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """
        GradleRunner runner = gradleRunner("myTask", '-i')
        Map<String, String> transformSnapshotAfterFirstBuild
        Map<String, String> transformSnapshotAfterSecondBuild

        when:
        withMockedDistributionDownload(runner) { GradleRunner effectiveRunner ->
            effectiveRunner.build()
            transformSnapshotAfterFirstBuild = snapshotTransformedDistribution()
            def secondBuild = effectiveRunner.build()
            transformSnapshotAfterSecondBuild = snapshotTransformedDistribution()
            secondBuild
        }

        then:
        assertCustomDistro('myCluster')
        transformSnapshotAfterSecondBuild == transformSnapshotAfterFirstBuild
        transformSnapshotAfterSecondBuild.keySet().any { it.endsWith('config/jvm.options') }
        transformSnapshotAfterSecondBuild.keySet().every { it.contains('logs/gc.log') == false }

    }

    def "three nodes use isolated distros and relative JVM paths across restart"() {
        given:
        buildFile << """
            testClusters {
              myCluster {
                testDistribution = 'archive'
                numberOfNodes = 3
              }
            }

            tasks.register('myTask', RestartingClusterAwareTask) {
                useCluster testClusters.myCluster
            }
        """

        when:
        def result = withMockedDistributionDownload(gradleRunner("myTask", '-i')) {
            build()
        }

        then:
        result.output.contains('RestartingClusterAwareTask executed')
        (0..<3).each { nodeIndex ->
            String nodeName = "myCluster-${nodeIndex}"
            assertCustomDistroForNode(nodeName)
            assertRelativeJvmOptions(nodeName)
            assertOpenSearchStdoutCount(nodeName, 'Starting OpenSearch process', 2)
            assertOpenSearchStdoutCount(nodeName, 'Stopping node', 2)
        }
        snapshotTransformedDistribution().keySet().every { it.contains('logs/gc.log') == false }
    }

    boolean assertOpenSearchStdoutContains(String testCluster, String expectedOutput) {
        assert new File(testProjectDir.root,
                "build/testclusters/${testCluster}-0/logs/opensearch.stdout.log").text.contains(expectedOutput)
        true
    }

    boolean assertOpenSearchStdoutCount(String nodeName, String expectedOutput, int expectedCount) {
        File stdout = new File(testProjectDir.root, "build/testclusters/${nodeName}/logs/opensearch.stdout.log")
        assert stdout.readLines().count { it.contains(expectedOutput) } == expectedCount
        true
    }

    boolean assertCustomDistro(String clusterName) {
        assertCustomDistroForNode("${clusterName}-0")
    }

    boolean assertCustomDistroForNode(String nodeName) {
        File distro = new File(testProjectDir.root, "build/testclusters/${nodeName}/distro")
        assert distro.isDirectory()
        assert distro.listFiles().find { new File(it, 'config/jvm.options').isFile() } != null
        true
    }

    boolean assertRelativeJvmOptions(String nodeName) {
        File jvmOptions = new File(testProjectDir.root, "build/testclusters/${nodeName}/config/jvm.options")
        assert jvmOptions.isFile()
        List<String> outputPathOptions = jvmOptions.readLines().findAll {
            it.contains('HeapDumpPath') || it.contains('logs/gc.log') || it.contains('ErrorFile')
        }
        assert outputPathOptions.any { it == '-XX:HeapDumpPath=logs' }
        assert outputPathOptions.any { it.contains('file=logs/gc.log') }
        assert outputPathOptions.any { it == '-XX:ErrorFile=logs/hs_err_pid%p.log' }
        assert outputPathOptions.every { it.contains(testProjectDir.root.absolutePath) == false }
        assert outputPathOptions.every { it.contains(testKitDir.absolutePath) == false }
        true
    }

    private Map<String, String> snapshotTransformedDistribution() {
        List<File> jvmOptionsFiles = []
        testKitDir.eachFileRecurse { file ->
            if (file.isFile()
                    && file.name == 'jvm.options'
                    && file.parentFile.name == 'config'
                    && file.toPath().any { it.toString().startsWith('transforms') }) {
                jvmOptionsFiles.add(file)
            }
        }
        Set<File> transformedDistributions = jvmOptionsFiles.collect { it.parentFile.parentFile } as Set
        assert transformedDistributions.empty == false

        Map<String, String> snapshot = new TreeMap<>()
        transformedDistributions.each { transformedDistribution ->
            String distributionPath = testKitDir.toPath().relativize(transformedDistribution.toPath()).toString().replace('\\', '/')
            snapshot.put(distributionPath, '<directory>')
            transformedDistribution.eachFileRecurse { file ->
                String relativePath = transformedDistribution.toPath().relativize(file.toPath()).toString().replace('\\', '/')
                snapshot.put(distributionPath + '/' + relativePath, file.isDirectory() ? '<directory>' : sha256(file))
            }
        }
        snapshot
    }

    private static String sha256(File file) {
        MessageDigest.getInstance('SHA-256').digest(file.bytes).encodeHex().toString()
    }
}
