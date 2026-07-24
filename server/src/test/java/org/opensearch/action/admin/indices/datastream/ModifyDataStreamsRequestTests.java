/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ModifyDataStreamsRequestTests extends OpenSearchTestCase {

    private DataStreamAction randomAction() {
        String ds = randomAlphaOfLength(6).toLowerCase(java.util.Locale.ROOT);
        String index = ".ds-" + ds + "-" + String.format(java.util.Locale.ROOT, "%06d", randomIntBetween(1, 1000));
        return randomBoolean() ? DataStreamAction.addBackingIndex(ds, index) : DataStreamAction.removeBackingIndex(ds, index);
    }

    public void testActionSerializationRoundTrip() throws IOException {
        DataStreamAction original = randomAction();
        DataStreamAction deserialized = copy(original);
        assertThat(deserialized, equalTo(original));
        assertThat(deserialized.hashCode(), equalTo(original.hashCode()));
    }

    public void testRequestSerializationRoundTrip() throws IOException {
        List<DataStreamAction> actions = new ArrayList<>();
        int count = randomIntBetween(1, 5);
        for (int i = 0; i < count; i++) {
            actions.add(randomAction());
        }
        ModifyDataStreamsAction.Request original = new ModifyDataStreamsAction.Request(actions);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ModifyDataStreamsAction.Request deserialized = new ModifyDataStreamsAction.Request(in);
                assertThat(deserialized, equalTo(original));
                assertThat(deserialized.getActions(), equalTo(original.getActions()));
            }
        }
    }

    public void testEmptyRequestFailsValidation() {
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(new ArrayList<>());
        assertNotNull(request.validate());
    }

    private DataStreamAction copy(DataStreamAction action) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            action.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new DataStreamAction(in);
            }
        }
    }
}
