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
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import picocli.CommandLine;

public abstract class FipsTrustStoreCommandTestCase extends OpenSearchTestCase {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    protected StringWriter outputCapture;
    protected StringWriter errorCapture;
    protected CommandLine commandLine;
    protected static Path sharedTempDir;

    @BeforeClass
    @SuppressForbidden(reason = "the java.io.File is exposed by TemporaryFolder")
    static void setUpClass() throws Exception {
        sharedTempDir = tempFolder.newFolder().toPath();
        setProperties();
    }

    @AfterClass
    static void tearDownClass() throws Exception {
        clearProperties();
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
