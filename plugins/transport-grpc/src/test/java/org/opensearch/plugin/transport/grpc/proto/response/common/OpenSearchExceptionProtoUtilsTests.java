/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response.common;

import org.opensearch.OpenSearchException;
import org.opensearch.protobufs.ErrorCause;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class OpenSearchExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithOpenSearchException() throws IOException {
        // Create an OpenSearchException
        OpenSearchException exception = new OpenSearchException("Test exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.toProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
        assertTrue("Should have a stack trace", errorCause.getStackTrace().length() > 0);
        assertFalse("Should not have suppressed exceptions", errorCause.getSuppressedList().iterator().hasNext());
        assertFalse("Should not have a cause", errorCause.hasCausedBy());
    }

    public void testToProtoWithNestedOpenSearchException() throws IOException {
        // Create a nested OpenSearchException
        IOException cause = new IOException("Cause exception");
        OpenSearchException exception = new OpenSearchException("Test exception", cause);

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.toProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
        assertTrue("Should have a stack trace", errorCause.getStackTrace().length() > 0);

        // Verify the cause
        assertTrue("Should have a cause", errorCause.hasCausedBy());
        ErrorCause causedBy = errorCause.getCausedBy();
        // The actual type format uses underscores instead of dots
        assertEquals("Cause should have the correct type", "i_o_exception", causedBy.getType());
        assertEquals("Cause should have the correct reason", "Cause exception", causedBy.getReason());
    }

    public void testGenerateThrowableProtoWithOpenSearchException() throws IOException {
        // Create an OpenSearchException
        OpenSearchException exception = new OpenSearchException("Test exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
    }

    public void testGenerateThrowableProtoWithIOException() throws IOException {
        // Create an IOException
        IOException exception = new IOException("Test IO exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "i_o_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test IO exception", errorCause.getReason());
    }

    public void testGenerateThrowableProtoWithRuntimeException() throws IOException {
        // Create a RuntimeException
        RuntimeException exception = new RuntimeException("Test runtime exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test runtime exception", errorCause.getReason());
    }

    public void testGenerateThrowableProtoWithNullMessage() throws IOException {
        // Create an exception with null message
        RuntimeException exception = new RuntimeException((String) null);

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertFalse("Should not have a reason", errorCause.hasReason());
    }

    public void testGenerateThrowableProtoWithSuppressedExceptions() throws IOException {
        // Create an exception with suppressed exceptions
        RuntimeException exception = new RuntimeException("Main exception");
        exception.addSuppressed(new IllegalArgumentException("Suppressed exception"));

        // Convert to Protocol Buffer
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Main exception", errorCause.getReason());

        // Verify suppressed exceptions
        assertEquals("Should have one suppressed exception", 1, errorCause.getSuppressedCount());
        ErrorCause suppressed = errorCause.getSuppressed(0);
        // The actual type format uses underscores instead of dots
        assertEquals("Suppressed should have the correct type", "illegal_argument_exception", suppressed.getType());
        assertEquals("Suppressed should have the correct reason", "Suppressed exception", suppressed.getReason());
    }

    public void testInnerToProtoWithBasicException() throws IOException {
        // Create a basic exception
        RuntimeException exception = new RuntimeException("Test exception");

        // Convert to Protocol Buffer using the protected method via reflection
        ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
        assertTrue("Should have a stack trace", errorCause.getStackTrace().length() > 0);
    }
}
