/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.concurrent.Callable;

public class FipsTrustStoreCommandTests extends FipsTrustStoreCommandTestCase {

    public void testWithUnmatchedArgs() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        var exitCode = commandLine.execute("--non-interactive", "--force", "-Ediscovery.type=single-node", "-Ehttp.port=9200");

        assertEquals(0, exitCode);
        assertTrue(errorCapture.toString().isEmpty());
        assertTrue(
            outputCapture.toString().contains("Warning: Ignoring unrecognized arguments: [-Ediscovery.type=single-node, -Ehttp.port=9200]")
        );
        assertTrue(outputCapture.toString().contains("Trust Store Configuration"));
        assertTrue(outputCapture.toString().contains("javax.net.ssl.trustStoreType: BCFKS"));
    }

    public void testWithEmptyUnmatchedArgs() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        var exitCode = commandLine.execute("--non-interactive", "--force");

        assertEquals(0, exitCode);
        assertTrue(errorCapture.toString().isEmpty());
        assertFalse(
            outputCapture.toString().contains("Warning: Ignoring unrecognized arguments: [-Ediscovery.type=single-node, -Ehttp.port=9200]")
        );
        assertTrue(outputCapture.toString().contains("Trust Store Configuration"));
        assertTrue(outputCapture.toString().contains("javax.net.ssl.trustStoreType: BCFKS"));
    }

    @Override
    Callable<Integer> getCut() {
        return new FipsTrustStoreCommand();
    }
}
