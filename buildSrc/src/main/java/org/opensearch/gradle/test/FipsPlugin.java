/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.test;

import groovy.lang.Closure;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.testing.Test;
import org.opensearch.gradle.testclusters.OpenSearchCluster;
import org.opensearch.gradle.testclusters.OpenSearchNode;
import org.opensearch.gradle.testclusters.TestClustersPlugin;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;

/**
 * This plugin automatically applies FIPS configuration to all test clusters when invoked with:
 * <code>./gradlew integTest -Pcrypto.standard=FIPS-140-3</code>
 * <p>
 * Features:
 * <pre>
 * - Configures test clusters with FIPS-compliant SSL/TLS settings
 * - Supports BouncyCastle FIPS provider (BCFKS keystore format)
 * - Works with both internal and external plugins
 * </pre>
 */
public class FipsPlugin implements Plugin<Project> {

    private static final String TRUSTSTORE_PASSWORD = "changeit";
    private static final String TRUSTSTORE_FILENAME = "opensearch-fips-truststore.bcfks";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PROVIDER = "javax.net.ssl.trustStoreProvider";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    @Override
    public void apply(Project project) {
        project.getLogger().lifecycle("FIPS mode enabled: Configuring test clusters for FIPS 140-3 compliance");
        File truststoreFile = findTruststoreFile(project);

        if (truststoreFile != null && truststoreFile.exists()) {
            project.getPlugins().withType(
                TestClustersPlugin.class, plugin -> {
                    configureTestClusters(project, truststoreFile);
                }
            );

            project.getTasks().withType(Test.class).configureEach(testTask -> {
                configureTestTask(project, testTask, truststoreFile);
            });
        }
    }

    private void configureTestClusters(Project project, File truststoreFile) {
        project.getExtensions().getByName("testClusters").getClass().getMethods();

        // Use dynamic approach since testClusters is a NamedDomainObjectContainer
        project.afterEvaluate(p -> {
            try {
                Object testClusters = p.getExtensions().getByName("testClusters");
                Method allMethod = testClusters.getClass().getMethod("all", groovy.lang.Closure.class);

                allMethod.invoke(
                    testClusters, new Closure<Void>(this) {
                        public void doCall(Object cluster) {
                            if (cluster instanceof OpenSearchCluster) {
                                configureSingleCluster(p, (OpenSearchCluster) cluster, truststoreFile);
                            }
                        }
                    }
                );
            } catch (Exception e) {
                p.getLogger().warn("Failed to configure FIPS for test clusters: {}", e.getMessage());
            }
        });
    }

    private void configureSingleCluster(Project project, OpenSearchCluster cluster, File truststoreFile) {
        // Set keystore password for the cluster
        cluster.keystorePassword(TRUSTSTORE_PASSWORD);

        // Add the truststore file to cluster config
        cluster.extraConfigFile(TRUSTSTORE_FILENAME, truststoreFile);

        // Configure SSL/TLS system properties for all nodes in the cluster
        for (OpenSearchNode node : cluster.getNodes()) {
            node.systemProperty(JAVAX_NET_SSL_TRUST_STORE, "${OPENSEARCH_PATH_CONF}/" + TRUSTSTORE_FILENAME);
            node.systemProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, "BCFKS");
            node.systemProperty(JAVAX_NET_SSL_TRUST_STORE_PROVIDER, "BCFIPS");
            node.systemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, TRUSTSTORE_PASSWORD);
        }

        project.getLogger().info("FIPS: Configured cluster '{}' with truststore from '{}'", cluster.getName(), truststoreFile);
    }

    private void configureTestTask(Project project, Test testTask, File truststoreFile) {
        testTask.systemProperty(JAVAX_NET_SSL_TRUST_STORE, truststoreFile.getAbsolutePath());
        testTask.systemProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, "BCFKS");
        testTask.systemProperty(JAVAX_NET_SSL_TRUST_STORE_PROVIDER, "BCFIPS");
        testTask.systemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, TRUSTSTORE_PASSWORD);

        project.getLogger().info("FIPS: Configured test task '{}' with truststore from '{}'", testTask.getName(), truststoreFile);
    }

    /**
     * Find the FIPS truststore file from classpath resources (packaged in build-tools.jar)
     * or fall back to filesystem locations for development builds.
     */
    private File findTruststoreFile(Project project) {
        // First, try to extract from classpath (works for external plugins using build-tools.jar)
        try {
            InputStream resourceStream = getClass().getClassLoader().getResourceAsStream(TRUSTSTORE_FILENAME);
            if (resourceStream != null) {
                // Extract to build directory
                File tempDir = new File(project.getBuildDir(), "fips");
                tempDir.mkdirs();
                File truststoreFile = new File(tempDir, TRUSTSTORE_FILENAME);

                // Only copy if it doesn't exist or is outdated
                if (!truststoreFile.exists()) {
                    java.nio.file.Files.copy(resourceStream, truststoreFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    project.getLogger().debug("FIPS: Extracted truststore from classpath to {}", truststoreFile);
                }
                resourceStream.close();
                return truststoreFile;
            }
        } catch (Exception e) {
            project.getLogger().debug("FIPS: Could not extract truststore from classpath: " + e.getMessage());
        }

        // Fallback: check filesystem locations (for development builds)
        // Location 1: buildSrc/src/main/resources (OpenSearch core development)
        File coreLocation = project.getRootProject().file("buildSrc/src/main/resources/" + TRUSTSTORE_FILENAME);
        if (coreLocation.exists()) {
            return coreLocation;
        }

        // Location 2: config/ directory
        File configLocation = project.getRootProject().file("config/" + TRUSTSTORE_FILENAME);
        if (configLocation.exists()) {
            return configLocation;
        }

        return null;
    }
}
