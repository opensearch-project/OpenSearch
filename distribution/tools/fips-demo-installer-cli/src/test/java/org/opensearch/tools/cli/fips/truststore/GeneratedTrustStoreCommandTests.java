/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.concurrent.Callable;

public class GeneratedTrustStoreCommandTests extends FipsTrustStoreCommandTestCase {

    public void testNonInteractiveModeAutoGeneratesPassword() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        int exitCode = commandLine.execute("--non-interactive", "--force");

        assertEquals(0, exitCode);
        var output = outputCapture.toString();
        assertTrue(output.contains("Auto-confirmed (non-interactive mode)"));
        assertTrue(output.contains("Generated secure password"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: BCFIPS"));
    }

    public void testNonInteractiveModeWithPasswordOption() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        int exitCode = commandLine.execute("--non-interactive", "--force", "--password", "MyPassword");

        assertEquals(0, exitCode);
        var output = outputCapture.toString();
        assertTrue(output.contains("Auto-confirmed (non-interactive mode)"));
        assertFalse(output.contains("Generated secure password"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: BCFIPS"));
    }

    public void testCommandWithEmptyPassword() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        int exitCode = commandLine.execute("--non-interactive", "--force", "--password", "");

        assertEquals(0, exitCode);
        var output = outputCapture.toString();
        assertTrue(output.contains("Auto-confirmed (non-interactive mode)"));
        assertFalse(output.contains("Generated secure password"));
        assertTrue(output.contains("WARNING: Using empty password"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: BCFIPS"));
    }

    @Override
    Callable<Integer> getCut() {
        return new GeneratedTrustStoreCommand();
    }
}
