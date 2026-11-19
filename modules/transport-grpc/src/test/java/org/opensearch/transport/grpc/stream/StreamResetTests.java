/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import org.opensearch.test.OpenSearchTestCase;

import io.grpc.ServerCall;

import static org.mockito.Mockito.mock;

public class StreamResetTests extends OpenSearchTestCase {

    /**
     * Fails when a grpc-java upgrade renames the internals stream resets depend on, instead of silently not resetting.
     */
    public void testGrpcInternalsAreAvailable() {
        assertTrue(StreamReset.isAvailable());
    }

    public void testDeclinesCallsOtherThanGrpcsOwn() {
        assertNull(StreamReset.of(mock(ServerCall.class)));
    }
}
