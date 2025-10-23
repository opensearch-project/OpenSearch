/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.cli.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import picocli.CommandLine;

public abstract class FipsTrustStoreCommandTestCase extends OpenSearchTestCase {

    protected StringWriter outputCapture;
    protected StringWriter errorCapture;
    protected CommandLine commandLine;
    protected static Path sharedTempDir;

    @BeforeClass
    static void setUpClass() throws Exception {
        sharedTempDir = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "system-command-test-");
        setProperties();
    }

    @AfterClass
    static void tearDownClass() throws Exception {
        clearProperties();
        if (sharedTempDir != null && Files.exists(sharedTempDir)) {
            try (var walk = Files.walk(sharedTempDir)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
            }
        }
    }

    @SuppressForbidden(reason = "set system properties as part of test setup")
    private static void setProperties() {
        System.setProperty("opensearch.path.conf", sharedTempDir.toString());
    }

    @SuppressForbidden(reason = "clear system properties")
    private static void clearProperties() {
        System.clearProperty("opensearch.path.conf");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        errorCapture = new StringWriter();
        outputCapture = new StringWriter();
        commandLine = new CommandLine(getCut()).setOut(new PrintWriter(outputCapture, true)).setErr(new PrintWriter(errorCapture, true));

        Path jvmOptionsFile = sharedTempDir.resolve("jvm.options");
        if (Files.exists(jvmOptionsFile)) {
            Files.delete(jvmOptionsFile);
        }
        Files.writeString(jvmOptionsFile, "# JVM Options\n-Xms1g\n-Xmx1g\n", StandardCharsets.UTF_8);
    }

    abstract Callable<Integer> getCut();

}
