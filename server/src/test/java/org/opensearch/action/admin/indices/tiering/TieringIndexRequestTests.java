/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class TieringIndexRequestTests extends OpenSearchTestCase {

    public void testTieringRequestWithListOfIndices() {
        TieringIndexRequest request = new TieringIndexRequest(
            TieringIndexRequest.Tier.WARM,
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            false,
            "foo",
            "bar",
            "baz"
        );
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);
    }

    public void testTieringRequestWithIndexPattern() {
        TieringIndexRequest request = new TieringIndexRequest(TieringIndexRequest.Tier.WARM.name(), "foo-*");
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);
    }

    public void testTieringRequestWithNullOrEmptyIndices() {
        TieringIndexRequest request = new TieringIndexRequest(TieringIndexRequest.Tier.WARM.name(), null, "");
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
    }

    public void testTieringRequestWithNotSupportedTier() {
        TieringIndexRequest request = new TieringIndexRequest(TieringIndexRequest.Tier.HOT.name(), "test");
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
    }

    public void testTieringTypeFromString() {
        expectThrows(IllegalArgumentException.class, () -> TieringIndexRequest.Tier.fromString("tier"));
        expectThrows(IllegalArgumentException.class, () -> TieringIndexRequest.Tier.fromString(null));
    }

    public void testSerDeOfTieringRequest() throws IOException {
        TieringIndexRequest request = new TieringIndexRequest(TieringIndexRequest.Tier.WARM.name(), "test");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final TieringIndexRequest deserializedRequest = new TieringIndexRequest(in);
                assertEquals(request, deserializedRequest);
            }
        }
    }

    public void testTieringRequestEquals() {
        final TieringIndexRequest original = new TieringIndexRequest(TieringIndexRequest.Tier.WARM.name(), "test");
        original.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        final TieringIndexRequest expected = new TieringIndexRequest(TieringIndexRequest.Tier.WARM.name(), original.indices());
        expected.indicesOptions(original.indicesOptions());
        assertThat(expected, equalTo(original));
        assertThat(expected.indices(), equalTo(original.indices()));
        assertThat(expected.indicesOptions(), equalTo(original.indicesOptions()));
    }
}
