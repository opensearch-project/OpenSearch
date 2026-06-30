/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.util;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import io.grpc.Status;

/**
 * Tests for RestToGrpcStatusConverter.
 * Validates that REST status codes are properly mapped to GRPC status codes.
 */
public class RestToGrpcStatusConverterTests extends OpenSearchTestCase {

    public void testSuccessStatusConversion() {
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.OK));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.CREATED));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.ACCEPTED));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.NO_CONTENT));
    }

    public void testClientErrorConversion() {
        assertEquals(Status.INVALID_ARGUMENT, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.BAD_REQUEST));
        assertEquals(Status.PERMISSION_DENIED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.UNAUTHORIZED));
        assertEquals(Status.PERMISSION_DENIED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.FORBIDDEN));
        assertEquals(Status.NOT_FOUND, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.NOT_FOUND));
        assertEquals(Status.UNIMPLEMENTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.METHOD_NOT_ALLOWED));
        assertEquals(Status.ABORTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.CONFLICT));
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.PRECONDITION_FAILED));
        assertEquals(Status.RESOURCE_EXHAUSTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.TOO_MANY_REQUESTS));
        assertEquals(Status.DEADLINE_EXCEEDED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.REQUEST_TIMEOUT));
    }

    public void testServerErrorConversion() {
        assertEquals(Status.INTERNAL, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.INTERNAL_SERVER_ERROR));
        assertEquals(Status.UNIMPLEMENTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.NOT_IMPLEMENTED));
        assertEquals(Status.UNAVAILABLE, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.BAD_GATEWAY));
        assertEquals(Status.UNAVAILABLE, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.SERVICE_UNAVAILABLE));
        assertEquals(Status.DEADLINE_EXCEEDED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.GATEWAY_TIMEOUT));
        assertEquals(Status.RESOURCE_EXHAUSTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.INSUFFICIENT_STORAGE));
    }

    public void testGrpcStatusCodeValues() {
        // Test that our getGrpcStatusCode method returns correct numeric values
        assertEquals(Status.OK.getCode().value(), RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.OK));
        assertEquals(Status.INVALID_ARGUMENT.getCode().value(), RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.BAD_REQUEST));
        assertEquals(Status.NOT_FOUND.getCode().value(), RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.NOT_FOUND));
        assertEquals(Status.PERMISSION_DENIED.getCode().value(), RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.FORBIDDEN));
        assertEquals(
            Status.RESOURCE_EXHAUSTED.getCode().value(),
            RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.TOO_MANY_REQUESTS)
        );
        assertEquals(Status.INTERNAL.getCode().value(), RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.INTERNAL_SERVER_ERROR));
        assertEquals(Status.UNAVAILABLE.getCode().value(), RestToGrpcStatusConverter.getGrpcStatusCode(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testAdditionalStatusConversion() {
        // 1xx Informational - now properly mapped
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.CONTINUE));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.SWITCHING_PROTOCOLS));

        // 2xx Success (additional codes)
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.NON_AUTHORITATIVE_INFORMATION));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.RESET_CONTENT));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.PARTIAL_CONTENT));
        assertEquals(Status.OK, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.MULTI_STATUS));

        // 3xx Redirects - now properly mapped
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.MULTIPLE_CHOICES));
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.MOVED_PERMANENTLY));
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.FOUND));
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.NOT_MODIFIED));

        // 4xx Additional client errors
        assertEquals(Status.PERMISSION_DENIED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.PAYMENT_REQUIRED));
        assertEquals(Status.NOT_FOUND, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.GONE));
        assertEquals(Status.UNAUTHENTICATED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.PROXY_AUTHENTICATION));
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.LENGTH_REQUIRED));
        assertEquals(Status.OUT_OF_RANGE, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertEquals(Status.OUT_OF_RANGE, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.REQUESTED_RANGE_NOT_SATISFIED));
        assertEquals(Status.INVALID_ARGUMENT, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.UNPROCESSABLE_ENTITY));
        assertEquals(Status.FAILED_PRECONDITION, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.LOCKED));

        // 5xx Additional server errors
        assertEquals(Status.UNIMPLEMENTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.HTTP_VERSION_NOT_SUPPORTED));
    }

    public void testCommonOpenSearchErrorMappings() {
        // Test mappings for common OpenSearch error scenarios
        assertEquals(Status.INVALID_ARGUMENT, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.BAD_REQUEST));
        assertEquals(Status.NOT_FOUND, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.NOT_FOUND));
        assertEquals(Status.ABORTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.CONFLICT));
        assertEquals(Status.RESOURCE_EXHAUSTED, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.TOO_MANY_REQUESTS));
        assertEquals(Status.UNAVAILABLE, RestToGrpcStatusConverter.convertRestToGrpcStatus(RestStatus.SERVICE_UNAVAILABLE));
    }
}
