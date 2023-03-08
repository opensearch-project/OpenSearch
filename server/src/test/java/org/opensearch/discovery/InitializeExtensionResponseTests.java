/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InitializeExtensionResponseTests extends OpenSearchTestCase {
    public void testInitializeExtensionResponse() throws Exception {
        String expectedName = "testsample-sdk";
        List<String> expectedImplementedInterfaces = new ArrayList<>(Arrays.asList("Action", "Search"));

        InitializeExtensionResponse initializeExtensionResponse = new InitializeExtensionResponse(
            expectedName,
            expectedImplementedInterfaces
        );

        assertEquals(expectedName, initializeExtensionResponse.getName());
        List<String> implementedInterfaces = initializeExtensionResponse.getImplementedInterfaces();
        assertEquals(expectedImplementedInterfaces.size(), implementedInterfaces.size());
        assertTrue(implementedInterfaces.containsAll(expectedImplementedInterfaces));
        assertTrue(expectedImplementedInterfaces.containsAll(implementedInterfaces));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            initializeExtensionResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                initializeExtensionResponse = new InitializeExtensionResponse(in);

                assertEquals(expectedName, initializeExtensionResponse.getName());
                implementedInterfaces = initializeExtensionResponse.getImplementedInterfaces();
                assertEquals(expectedImplementedInterfaces.size(), implementedInterfaces.size());
                assertTrue(implementedInterfaces.containsAll(expectedImplementedInterfaces));
                assertTrue(expectedImplementedInterfaces.containsAll(implementedInterfaces));
            }
        }
    }

}
