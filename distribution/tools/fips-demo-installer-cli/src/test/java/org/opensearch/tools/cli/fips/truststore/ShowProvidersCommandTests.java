/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.concurrent.Callable;

public class ShowProvidersCommandTests extends FipsTrustStoreCommandTestCase {

    public void testCallOutputsProviderInformation() {
        commandLine.execute();

        int exitCode = commandLine.execute();
        assertEquals(0, exitCode);
        assertTrue(errorCapture.toString().isEmpty());
        assertTrue(outputCapture.toString().contains("Available Security Providers"));
    }

    @Override
    Callable<Integer> getCut() {
        return new ShowProvidersCommand();
    }
}
