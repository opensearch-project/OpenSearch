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
 * Tests for HttpToGrpcStatusConverter.
 * Validates that HTTP status codes are properly mapped to GRPC status codes.
 */
public class HttpToGrpcStatusConverterTests extends OpenSearchTestCase {

    public void testSuccessStatusConversion() {
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.OK));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.CREATED));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.ACCEPTED));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.NO_CONTENT));
    }

    public void testClientErrorConversion() {
        assertEquals(Status.INVALID_ARGUMENT, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.BAD_REQUEST));
        assertEquals(Status.UNAUTHENTICATED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.UNAUTHORIZED));
        assertEquals(Status.PERMISSION_DENIED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.FORBIDDEN));
        assertEquals(Status.NOT_FOUND, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.NOT_FOUND));
        assertEquals(Status.UNIMPLEMENTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.METHOD_NOT_ALLOWED));
        assertEquals(Status.ABORTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.CONFLICT));
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.PRECONDITION_FAILED));
        assertEquals(Status.RESOURCE_EXHAUSTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.TOO_MANY_REQUESTS));
        assertEquals(Status.DEADLINE_EXCEEDED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.REQUEST_TIMEOUT));
    }

    public void testServerErrorConversion() {
        assertEquals(Status.INTERNAL, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.INTERNAL_SERVER_ERROR));
        assertEquals(Status.UNIMPLEMENTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.NOT_IMPLEMENTED));
        assertEquals(Status.UNAVAILABLE, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.BAD_GATEWAY));
        assertEquals(Status.UNAVAILABLE, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.SERVICE_UNAVAILABLE));
        assertEquals(Status.DEADLINE_EXCEEDED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.GATEWAY_TIMEOUT));
        assertEquals(Status.RESOURCE_EXHAUSTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.INSUFFICIENT_STORAGE));
    }

    public void testGrpcStatusCodeValues() {
        // Test that our getGrpcStatusCode method returns correct numeric values
        assertEquals(Status.OK.getCode().value(), HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.OK));
        assertEquals(Status.INVALID_ARGUMENT.getCode().value(), HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.BAD_REQUEST));
        assertEquals(Status.NOT_FOUND.getCode().value(), HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.NOT_FOUND));
        assertEquals(Status.PERMISSION_DENIED.getCode().value(), HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.FORBIDDEN));
        assertEquals(
            Status.RESOURCE_EXHAUSTED.getCode().value(),
            HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.TOO_MANY_REQUESTS)
        );
        assertEquals(Status.INTERNAL.getCode().value(), HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.INTERNAL_SERVER_ERROR));
        assertEquals(Status.UNAVAILABLE.getCode().value(), HttpToGrpcStatusConverter.getGrpcStatusCode(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testAdditionalStatusConversion() {
        // 1xx Informational - now properly mapped
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.CONTINUE));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.SWITCHING_PROTOCOLS));

        // 2xx Success (additional codes)
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.NON_AUTHORITATIVE_INFORMATION));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.RESET_CONTENT));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.PARTIAL_CONTENT));
        assertEquals(Status.OK, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.MULTI_STATUS));

        // 3xx Redirects - now properly mapped
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.MULTIPLE_CHOICES));
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.MOVED_PERMANENTLY));
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.FOUND));
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.NOT_MODIFIED));

        // 4xx Additional client errors
        assertEquals(Status.PERMISSION_DENIED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.PAYMENT_REQUIRED));
        assertEquals(Status.NOT_FOUND, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.GONE));
        assertEquals(Status.UNAUTHENTICATED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.PROXY_AUTHENTICATION));
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.LENGTH_REQUIRED));
        assertEquals(Status.OUT_OF_RANGE, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertEquals(Status.OUT_OF_RANGE, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.REQUESTED_RANGE_NOT_SATISFIED));
        assertEquals(Status.INVALID_ARGUMENT, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.UNPROCESSABLE_ENTITY));
        assertEquals(Status.FAILED_PRECONDITION, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.LOCKED));

        // 5xx Additional server errors
        assertEquals(Status.UNIMPLEMENTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.HTTP_VERSION_NOT_SUPPORTED));
    }

    public void testCommonOpenSearchErrorMappings() {
        // Test mappings for common OpenSearch error scenarios
        assertEquals(Status.INVALID_ARGUMENT, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.BAD_REQUEST));
        assertEquals(Status.NOT_FOUND, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.NOT_FOUND));
        assertEquals(Status.ABORTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.CONFLICT));
        assertEquals(Status.RESOURCE_EXHAUSTED, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.TOO_MANY_REQUESTS));
        assertEquals(Status.UNAVAILABLE, HttpToGrpcStatusConverter.convertHttpToGrpcStatus(RestStatus.SERVICE_UNAVAILABLE));
    }
}
