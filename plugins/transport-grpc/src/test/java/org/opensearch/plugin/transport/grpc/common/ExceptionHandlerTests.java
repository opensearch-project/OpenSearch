/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.common;

import org.opensearch.test.OpenSearchTestCase;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class ExceptionHandlerTests extends OpenSearchTestCase {

    public void testAnnotateExceptionWithIllegalArgumentException() {
        // Test with an IllegalArgumentException (should map to INVALID_ARGUMENT)
        IllegalArgumentException exception = new IllegalArgumentException("Invalid argument");
        Throwable annotated = ExceptionHandler.annotateException(exception);

        assertTrue("Should return a StatusRuntimeException", annotated instanceof StatusRuntimeException);
        StatusRuntimeException statusException = (StatusRuntimeException) annotated;
        assertEquals("Should have INVALID_ARGUMENT status", Status.Code.INVALID_ARGUMENT, statusException.getStatus().getCode());
        assertTrue("Should contain the original message", statusException.getMessage().contains("Invalid argument"));
    }

    public void testAnnotateExceptionWithNoSuchElementException() {
        // Test with a NoSuchElementException (should map to NOT_FOUND)
        NoSuchElementException exception = new NoSuchElementException("Element not found");
        Throwable annotated = ExceptionHandler.annotateException(exception);

        assertTrue("Should return a StatusRuntimeException", annotated instanceof StatusRuntimeException);
        StatusRuntimeException statusException = (StatusRuntimeException) annotated;
        assertEquals("Should have NOT_FOUND status", Status.Code.NOT_FOUND, statusException.getStatus().getCode());
        assertTrue("Should contain the original message", statusException.getMessage().contains("Element not found"));
    }

    public void testAnnotateExceptionWithTimeoutException() {
        // Test with a TimeoutException (should map to DEADLINE_EXCEEDED)
        TimeoutException exception = new TimeoutException("Operation timed out");
        Throwable annotated = ExceptionHandler.annotateException(exception);

        assertTrue("Should return a StatusRuntimeException", annotated instanceof StatusRuntimeException);
        StatusRuntimeException statusException = (StatusRuntimeException) annotated;
        assertEquals("Should have DEADLINE_EXCEEDED status", Status.Code.DEADLINE_EXCEEDED, statusException.getStatus().getCode());
        assertTrue("Should contain the original message", statusException.getMessage().contains("Operation timed out"));
    }

    public void testAnnotateExceptionWithUnsupportedOperationException() {
        // Test with an UnsupportedOperationException (should map to UNIMPLEMENTED)
        UnsupportedOperationException exception = new UnsupportedOperationException("Operation not supported");
        Throwable annotated = ExceptionHandler.annotateException(exception);

        assertTrue("Should return a StatusRuntimeException", annotated instanceof StatusRuntimeException);
        StatusRuntimeException statusException = (StatusRuntimeException) annotated;
        assertEquals("Should have UNIMPLEMENTED status", Status.Code.UNIMPLEMENTED, statusException.getStatus().getCode());
        assertTrue("Should contain the original message", statusException.getMessage().contains("Operation not supported"));
    }

    public void testAnnotateExceptionWithRuntimeException() {
        // Test with a generic RuntimeException (should map to INTERNAL)
        RuntimeException exception = new RuntimeException("Runtime error");
        Throwable annotated = ExceptionHandler.annotateException(exception);

        assertTrue("Should return a StatusRuntimeException", annotated instanceof StatusRuntimeException);
        StatusRuntimeException statusException = (StatusRuntimeException) annotated;
        assertEquals("Should have INTERNAL status", Status.Code.INTERNAL, statusException.getStatus().getCode());
        assertTrue("Should contain the original message", statusException.getMessage().contains("Runtime error"));
    }

    public void testAnnotateExceptionWithStatusRuntimeException() {
        // Test with an already annotated exception (should return the same exception)
        StatusRuntimeException original = Status.INTERNAL.withDescription("Already annotated").asRuntimeException();
        Throwable annotated = ExceptionHandler.annotateException(original);

        assertSame("Should return the same exception", original, annotated);
    }
}
