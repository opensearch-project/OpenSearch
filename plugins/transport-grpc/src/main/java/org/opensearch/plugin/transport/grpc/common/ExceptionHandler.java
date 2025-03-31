/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.common;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;

/**
 * Handler for converting Java exceptions to gRPC Status exceptions.
 * This utility class provides methods to map Java exceptions to appropriate gRPC status codes.
 * It contains only static methods and is not meant to be instantiated.
 */
public class ExceptionHandler {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private ExceptionHandler() {
        // This is a utility class and should not be instantiated
    }

    /**
     * Maps specific Java exceptions to gRPC status codes.
     * This method is similar to the status() method in ExceptionsHelper.java.
     *
     * @param t The exception to convert
     * @return A gRPC Status exception
     */
    public static Throwable annotateException(Throwable t) {
        // If it's already a StatusRuntimeException, return it as is
        if (t instanceof io.grpc.StatusRuntimeException) {
            return t;
        }

        // TODO add more exception to GRPC status code mappings
        if (t instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT.withDescription(t.getMessage()).withCause(t).asRuntimeException();
        } else if (t instanceof NoSuchElementException) {
            return Status.NOT_FOUND.withDescription(t.getMessage()).withCause(t).asRuntimeException();
        } else if (t instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED.withDescription(t.getMessage()).withCause(t).asRuntimeException();
        } else if (t instanceof OpenSearchRejectedExecutionException) {
            return Status.RESOURCE_EXHAUSTED.withDescription(t.getMessage()).withCause(t).asRuntimeException();
        } else if (t instanceof UnsupportedOperationException) {
            return Status.UNIMPLEMENTED.withDescription(t.getMessage()).withCause(t).asRuntimeException();
        } else {
            return Status.INTERNAL.withDescription(t.getMessage()).withCause(t).asRuntimeException();
        }
    }
}
