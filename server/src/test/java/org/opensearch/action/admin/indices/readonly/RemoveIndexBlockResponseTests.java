/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.action.admin.indices.readonly.RemoveIndexBlockResponse.RemoveBlockResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class RemoveIndexBlockResponseTests extends OpenSearchTestCase {

    private static RemoveIndexBlockResponse roundTrip(RemoveIndexBlockResponse response) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new RemoveIndexBlockResponse(in);
            }
        }
    }

    public void testSerialization() throws IOException {
        final List<RemoveBlockResult> results = new ArrayList<>();
        final int count = randomIntBetween(0, 5);
        for (int i = 0; i < count; i++) {
            final Index index = new Index("index-" + i, randomAlphaOfLength(22));
            results.add(
                randomBoolean() ? new RemoveBlockResult(index) : new RemoveBlockResult(index, new IllegalStateException("boom " + i))
            );
        }
        final boolean acknowledged = randomBoolean();

        final RemoveIndexBlockResponse deserialized = roundTrip(new RemoveIndexBlockResponse(acknowledged, results));

        assertThat(deserialized.isAcknowledged(), equalTo(acknowledged));
        assertThat(deserialized.getIndices(), hasSize(count));
        for (int i = 0; i < count; i++) {
            final RemoveBlockResult expected = results.get(i);
            final RemoveBlockResult actual = deserialized.getIndices().get(i);
            assertThat(actual.getIndex(), equalTo(expected.getIndex()));
            assertThat(actual.isUnblocked(), equalTo(expected.isUnblocked()));
            if (expected.getException() == null) {
                assertNull(actual.getException());
            } else {
                assertThat(actual.getException(), instanceOf(IllegalStateException.class));
                assertThat(actual.getException().getMessage(), equalTo(expected.getException().getMessage()));
            }
        }
    }

    public void testToXContentSuccess() {
        final RemoveIndexBlockResponse response = new RemoveIndexBlockResponse(
            true,
            List.of(new RemoveBlockResult(new Index("logs-2026.11", "uuid-1")), new RemoveBlockResult(new Index("logs-2026.12", "uuid-2")))
        );
        assertThat(
            Strings.toString(MediaTypeRegistry.JSON, response),
            equalTo(
                "{\"acknowledged\":true,\"indices\":["
                    + "{\"name\":\"logs-2026.11\",\"unblocked\":true},"
                    + "{\"name\":\"logs-2026.12\",\"unblocked\":true}]}"
            )
        );
    }

    public void testToXContentWithoutIndices() {
        assertThat(
            Strings.toString(MediaTypeRegistry.JSON, new RemoveIndexBlockResponse(true, Collections.emptyList())),
            equalTo("{\"acknowledged\":true,\"indices\":[]}")
        );
    }

    public void testToXContentNotAcknowledged() {
        final RemoveIndexBlockResponse response = new RemoveIndexBlockResponse(false, List.of(new RemoveBlockResult(new Index("a", "u"))));
        assertThat(
            Strings.toString(MediaTypeRegistry.JSON, response),
            equalTo("{\"acknowledged\":false,\"indices\":[{\"name\":\"a\",\"unblocked\":true}]}")
        );
    }

    public void testToXContentFailureKeepsUnblockedFlagAndReportsException() {
        final RemoveIndexBlockResponse response = new RemoveIndexBlockResponse(
            false,
            List.of(new RemoveBlockResult(new Index("stuck", "u"), new IllegalStateException("temporary block present")))
        );
        assertThat(
            Strings.toString(MediaTypeRegistry.JSON, response),
            equalTo(
                "{\"acknowledged\":false,\"indices\":[{\"name\":\"stuck\",\"unblocked\":false,\"exception\":{\"error\":{"
                    + "\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"temporary block present\"}],"
                    + "\"type\":\"illegal_state_exception\",\"reason\":\"temporary block present\"}}}]}"
            )
        );
    }

    public void testIndicesListIsImmutable() {
        final RemoveIndexBlockResponse response = new RemoveIndexBlockResponse(true, new ArrayList<>());
        expectThrows(UnsupportedOperationException.class, () -> response.getIndices().add(new RemoveBlockResult(new Index("a", "u"))));
    }

    public void testNullIndicesAreRejected() {
        expectThrows(NullPointerException.class, () -> new RemoveIndexBlockResponse(true, null));
        expectThrows(NullPointerException.class, () -> new RemoveBlockResult((Index) null));
        expectThrows(NullPointerException.class, () -> new RemoveBlockResult(null, new IllegalStateException("x")));
        expectThrows(NullPointerException.class, () -> new RemoveBlockResult(new Index("a", "u"), null));
    }
}
