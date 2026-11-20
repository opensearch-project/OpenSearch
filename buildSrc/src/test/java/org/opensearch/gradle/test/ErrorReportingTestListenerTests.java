/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.test;

import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.api.tasks.testing.logging.TestLogging;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorReportingTestListenerTests extends GradleUnitTestCase {
    public void testSuitesWithoutClassNames() throws Exception {
        assertSeparateOutput(null);
    }

    public void testSuitesWithSameClassName() throws Exception {
        assertSeparateOutput("example.TestClass");
    }

    private void assertSeparateOutput(String className) throws Exception {
        File directory = Files.createTempDirectory("test-output").toFile();
        ErrorReportingTestListener listener = new ErrorReportingTestListener(mock(TestLogging.class), mock(Logger.class), directory);
        TestDescriptor first = suite("first", className);
        TestDescriptor second = suite("second", className);
        TestResult failure = mock(TestResult.class);
        when(failure.getResultType()).thenReturn(TestResult.ResultType.FAILURE);
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream output = new PrintStream(captured, false, StandardCharsets.UTF_8)) {
            System.setOut(output);
            listener.onOutput(first, event("first output\n"));
            listener.onOutput(second, event("second output\n"));
            listener.afterSuite(first, failure);
            assertEquals("  1> first output" + System.lineSeparator(), captured.toString(StandardCharsets.UTF_8));
            captured.reset();
            listener.afterSuite(second, failure);
            assertEquals("  1> second output" + System.lineSeparator(), captured.toString(StandardCharsets.UTF_8));
            assertEquals(0, directory.list().length);
        } finally {
            System.setOut(original);
            listener.afterSuite(first, failure);
            listener.afterSuite(second, failure);
            Files.delete(directory.toPath());
        }
    }

    private TestDescriptor suite(String name, String className) {
        TestDescriptor suite = mock(TestDescriptor.class);
        when(suite.getName()).thenReturn(name);
        when(suite.getClassName()).thenReturn(className);
        when(suite.isComposite()).thenReturn(true);
        return suite;
    }

    private TestOutputEvent event(String message) {
        TestOutputEvent event = mock(TestOutputEvent.class);
        when(event.getMessage()).thenReturn(message);
        when(event.getDestination()).thenReturn(TestOutputEvent.Destination.StdOut);
        return event;
    }
}
