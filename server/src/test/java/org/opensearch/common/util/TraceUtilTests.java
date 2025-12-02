/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

public class TraceUtilTests extends OpenSearchTestCase {

    public void testValidTraceparent() {
        String validTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        String traceId = TraceUtil.extractTraceId(validTraceparent);
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", traceId);
    }

    public void testWithNullTraceparent() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { TraceUtil.extractTraceId(null); });
        assertEquals("traceparent cannot be null", exception.getMessage());
    }

    public void testWithInvalidLength() {
        // Too short
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7");
        });
        assertEquals("traceparent must be exactly 55 characters long", exception.getMessage());

        // Too long
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra");
        });
        assertEquals("traceparent must be exactly 55 characters long", exception.getMessage());
    }

    public void testWithInvalidNumberOfParts() {
        // Too many parts (correct length but 5 parts instead of 4)
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-bf-92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        });
        assertEquals("traceparent must have exactly 4 parts separated by '-'", exception.getMessage());
    }

    public void testWithInvalidVersion() {
        // Version too short
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("0-4bf92f3577b34da6a3ce929d0e0e47360-00f067aa0ba902b7-01");
        });
        assertEquals("version must be 2 hex digits", exception.getMessage());

        // Version too long
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("000-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        });
        assertEquals("traceparent must be exactly 55 characters long", exception.getMessage());

        // Version with invalid hex
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("0g-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        });
        assertEquals("version must be 2 hex digits", exception.getMessage());
    }

    public void testWithInvalidTraceId() {
        // Trace ID too short
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b70-01");
        });
        assertEquals("trace_id must be 32 hex digits", exception.getMessage());

        // Trace ID too long
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e47360-00f067aa0ba902b-01");
        });
        assertEquals("trace_id must be 32 hex digits", exception.getMessage());

        // Trace ID with invalid hex
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01");
        });
        assertEquals("trace_id must be 32 hex digits", exception.getMessage());

        // Trace ID all zeros
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-00000000000000000000000000000000-00f067aa0ba902b7-01");
        });
        assertEquals("trace_id cannot be all zeros", exception.getMessage());
    }

    public void testWithInvalidParentId() {
        // Parent ID too short
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-010");
        });
        assertEquals("parent_id must be 16 hex digits", exception.getMessage());

        // Parent ID too long
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b70-1");
        });
        assertEquals("parent_id must be 16 hex digits", exception.getMessage());

        // Parent ID with invalid hex
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bg-01");
        });
        assertEquals("parent_id must be 16 hex digits", exception.getMessage());

        // Parent ID all zeros
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01");
        });
        assertEquals("parent_id cannot be all zeros", exception.getMessage());
    }

    public void testWithInvalidTraceFlags() {
        // Trace flags too short
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1");
        });
        assertEquals("traceparent must be exactly 55 characters long", exception.getMessage());

        // Trace flags too long
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-010");
        });
        assertEquals("traceparent must be exactly 55 characters long", exception.getMessage());

        // Trace flags with invalid hex
        exception = expectThrows(IllegalArgumentException.class, () -> {
            TraceUtil.extractTraceId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g");
        });
        assertEquals("trace_flags must be 2 hex digits", exception.getMessage());
    }

    public void testWithValidHexVariations() {
        // Test with lowercase hex
        String traceparent1 = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", TraceUtil.extractTraceId(traceparent1));

        // Test with uppercase hex
        String traceparent2 = "FF-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-FF";
        assertEquals("4BF92F3577B34DA6A3CE929D0E0E4736", TraceUtil.extractTraceId(traceparent2));

        // Test with mixed case hex
        String traceparent3 = "aB-4bF92f3577B34dA6a3cE929d0E0e4736-00F067aa0Ba902B7-cD";
        assertEquals("4bF92f3577B34dA6a3cE929d0E0e4736", TraceUtil.extractTraceId(traceparent3));
    }

    public void testSomeEdgeCases() {
        // Minimum valid values (version 00, non-zero trace_id and parent_id, flags 00)
        String minValid = "00-00000000000000000000000000000001-0000000000000001-00";
        assertEquals("00000000000000000000000000000001", TraceUtil.extractTraceId(minValid));

        // Maximum valid values
        String maxValid = "ff-ffffffffffffffffffffffffffffffff-ffffffffffffffff-ff";
        assertEquals("ffffffffffffffffffffffffffffffff", TraceUtil.extractTraceId(maxValid));
    }
}
