/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.protobufs.CreatePitRequest.Builder;
import org.opensearch.protobufs.ExpandWildcard;
import org.opensearch.protobufs.GlobalParams;
import org.opensearch.test.OpenSearchTestCase;

public class CreatePitRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testPrepareRequestWithBasicFields() {
        Builder requestBuilder = org.opensearch.protobufs.CreatePitRequest.newBuilder()
            .addIndex("index-1")
            .addIndex("index-2")
            .setAllowNoIndices(false)
            .setAllowPartialPitCreation(false)
            .addExpandWildcards(ExpandWildcard.EXPAND_WILDCARD_OPEN)
            .addExpandWildcards(ExpandWildcard.EXPAND_WILDCARD_CLOSED)
            .setIgnoreThrottled(true)
            .setIgnoreUnavailable(true)
            .setKeepAlive("5m")
            .setPreference("_local")
            .addRouting("route-1")
            .addRouting("route-2");

        CreatePitRequest request = CreatePitRequestProtoUtils.prepareRequest(requestBuilder.build());

        assertArrayEquals(new String[] { "index-1", "index-2" }, request.getIndices());
        assertFalse(request.shouldAllowPartialPitCreation());
        assertEquals(TimeValue.timeValueMinutes(5), request.getKeepAlive());
        assertEquals("_local", request.getPreference());
        assertEquals("route-1,route-2", request.getRouting());
        assertTrue(request.getIndicesOptions().ignoreUnavailable());
        assertFalse(request.getIndicesOptions().allowNoIndices());
        assertTrue(request.getIndicesOptions().ignoreThrottled());
        assertTrue(request.getIndicesOptions().expandWildcardsOpen());
        assertTrue(request.getIndicesOptions().expandWildcardsClosed());
    }

    public void testPrepareRequestDefaultsAllowPartialPitCreationToTrue() {
        CreatePitRequest request = CreatePitRequestProtoUtils.prepareRequest(
            org.opensearch.protobufs.CreatePitRequest.newBuilder().addIndex("index-1").setKeepAlive("1m").build()
        );

        assertTrue(request.shouldAllowPartialPitCreation());
        assertEquals(TimeValue.timeValueMinutes(1), request.getKeepAlive());
    }

    public void testPrepareRequestUsesDefaultIndicesOptionsWhenFlagsAreOmitted() {
        CreatePitRequest request = CreatePitRequestProtoUtils.prepareRequest(
            org.opensearch.protobufs.CreatePitRequest.newBuilder()
                .addIndex("index-1")
                .setKeepAlive("1m")
                .addExpandWildcards(ExpandWildcard.EXPAND_WILDCARD_OPEN)
                .build()
        );

        assertFalse(request.getIndicesOptions().ignoreUnavailable());
        assertTrue(request.getIndicesOptions().allowNoIndices());
        assertTrue(request.getIndicesOptions().ignoreThrottled());
        assertTrue(request.getIndicesOptions().expandWildcardsOpen());
    }

    public void testPrepareRequestAllowsEmptyKeepAlive() {
        CreatePitRequest request = CreatePitRequestProtoUtils.prepareRequest(
            org.opensearch.protobufs.CreatePitRequest.newBuilder().addIndex("index-1").build()
        );

        assertNull(request.getKeepAlive());
    }

    public void testPrepareRequestRejectsGlobalParams() {
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> CreatePitRequestProtoUtils.prepareRequest(
                org.opensearch.protobufs.CreatePitRequest.newBuilder()
                    .addIndex("index-1")
                    .setKeepAlive("1m")
                    .setGlobalParams(GlobalParams.newBuilder().build())
                    .build()
            )
        );

        assertTrue(exception.getMessage().contains("global_params"));
    }
}
