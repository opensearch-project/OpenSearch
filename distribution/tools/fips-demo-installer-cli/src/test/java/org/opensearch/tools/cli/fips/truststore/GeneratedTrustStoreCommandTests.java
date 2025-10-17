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

    public void testCommandWithForceOption() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        int exitCode = commandLine.execute("--non-interactive", "--force");

        assertEquals(0, exitCode);
        var output = outputCapture.toString();
        assertTrue(output.contains("Auto-confirmed (non-interactive mode)"));
        assertTrue(output.contains("Generated secure password"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: BCFIPS"));
    }

    @Override
    Callable<Integer> getCut() {
        return new GeneratedTrustStoreCommand();
    }
}
