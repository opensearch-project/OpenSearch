/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

import java.io.PrintWriter;
import java.io.StringWriter;

import picocli.CommandLine;

public class FipsTrustStoreExceptionHandlerTests extends OpenSearchTestCase {

    public void testPrintsErrorMessage() {
        // given
        var handler = new FipsTrustStoreExceptionHandler();
        var errorCapture = new StringWriter();

        var commandLine = new CommandLine(new FipsTrustStoreCommand()).setErr(new PrintWriter(errorCapture, true));

        var testException = new RuntimeException("Test error message");

        // when
        int exitCode = handler.handleExecutionException(testException, commandLine, null);

        // then
        var errorOutput = errorCapture.toString();
        assertEquals(1, exitCode);
        assertEquals("Error: Test error message\n", errorOutput);
    }

    public void testPrintsNullErrorMessage() {
        // given
        var handler = new FipsTrustStoreExceptionHandler();
        var errorCapture = new StringWriter();

        var commandLine = new CommandLine(new FipsTrustStoreCommand()).setErr(new PrintWriter(errorCapture, true));

        var testException = new RuntimeException();

        // when
        int exitCode = handler.handleExecutionException(testException, commandLine, null);

        // then
        var errorOutput = errorCapture.toString();
        assertEquals(1, exitCode);
        assertEquals("Error: [null]\n", errorOutput);
    }
}
