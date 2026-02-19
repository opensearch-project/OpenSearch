/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

public class TrustStoreServiceFipsTests extends TrustStoreServiceTests {

    public void testExecuteInteractiveSelectionNonInteractiveMode() {
        // given
        var options = new CommonOptions();
        options.nonInteractive = true;
        var userInteraction = createUserInteractionService("password123\npassword123\n");
        var service = new TrustStoreService(userInteraction);

        // when
        var result = service.executeInteractiveSelection(spec, options, sharedTempDir);

        // then
        assertTrue(outputCapture.toString().contains("Non-interactive mode: Using generated trust store (default)"));
        assertNotNull(result);
    }

}
