/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

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

    /**
     * The empty-request validation failure carries the documented message. Covers the {@code actions.isEmpty()} true
     * branch of Request#validate().
     */
    public void testEmptyRequestValidationMessage() {
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(Collections.emptyList());
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.validationErrors().get(0), containsString("at least one data stream action must be specified"));
    }

    /**
     * A request with at least one action validates cleanly. Covers the {@code actions.isEmpty()} false branch, i.e.
     * the {@code return null} path of Request#validate().
     */
    public void testNonEmptyRequestPassesValidation() {
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(
            Collections.singletonList(DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"))
        );
        assertNull(request.validate());
    }

    /**
     * The constructor rejects a null action list, so {@code actions == null} can never be observed by validate().
     */
    public void testNullActionListRejectedByConstructor() {
        final List<DataStreamAction> nullActions = null;
        NullPointerException e = expectThrows(NullPointerException.class, () -> new ModifyDataStreamsAction.Request(nullActions));
        assertThat(e.getMessage(), containsString("actions must not be null"));
    }

    /**
     * The constructor defensively copies the caller's list: later mutation of the source list is not visible.
     */
    public void testConstructorCopiesActionList() {
        List<DataStreamAction> source = new ArrayList<>();
        source.add(DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"));
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(source);

        source.add(DataStreamAction.removeBackingIndex("logs-foo", ".ds-logs-foo-000002"));

        assertThat(request.getActions(), hasSize(1));
        assertThat(request.getActions(), contains(DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001")));
    }

    /**
     * Drives every branch of Request#equals(Object): identity, null, foreign class, unequal actions and equal actions.
     */
    public void testEqualsAndHashCode() {
        DataStreamAction add = DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001");
        DataStreamAction remove = DataStreamAction.removeBackingIndex("logs-foo", ".ds-logs-foo-000002");

        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(Collections.singletonList(add));
        ModifyDataStreamsAction.Request same = new ModifyDataStreamsAction.Request(Collections.singletonList(add));
        ModifyDataStreamsAction.Request different = new ModifyDataStreamsAction.Request(Collections.singletonList(remove));
        ModifyDataStreamsAction.Request longer = new ModifyDataStreamsAction.Request(Arrays.asList(add, remove));

        // this == o
        assertTrue(request.equals(request));
        // o == null
        assertFalse(request.equals(null));
        // getClass() != o.getClass()
        assertFalse(request.equals("not a request"));
        // actions.equals(...) == false, same size
        assertThat(request, not(equalTo(different)));
        // actions.equals(...) == false, different size
        assertThat(request, not(equalTo(longer)));
        // actions.equals(...) == true
        assertThat(request, equalTo(same));
        assertThat(request.hashCode(), equalTo(same.hashCode()));
    }

    /**
     * Non-default acknowledgement and cluster-manager timeouts survive the wire round trip, exercising the
     * {@code super.writeTo(out)} / {@code super(in)} halves of the request serialization.
     */
    public void testTimeoutsSurviveSerialization() throws IOException {
        ModifyDataStreamsAction.Request original = new ModifyDataStreamsAction.Request(
            Collections.singletonList(DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"))
        );
        original.timeout(TimeValue.timeValueSeconds(7));
        original.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(11));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ModifyDataStreamsAction.Request deserialized = new ModifyDataStreamsAction.Request(in);
                assertThat(deserialized.timeout(), equalTo(TimeValue.timeValueSeconds(7)));
                assertThat(deserialized.clusterManagerNodeTimeout(), equalTo(TimeValue.timeValueSeconds(11)));
                assertThat(deserialized, equalTo(original));
            }
        }
    }

    /**
     * A freshly built request carries the standard acknowledged-request default ack timeout.
     */
    public void testDefaultTimeouts() {
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(
            Collections.singletonList(DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"))
        );
        assertThat(request.timeout(), equalTo(AcknowledgedRequest.DEFAULT_ACK_TIMEOUT));
    }

    /**
     * The action singleton exposes the registered transport name and a reader that yields an AcknowledgedResponse.
     */
    public void testActionTypeSingleton() throws IOException {
        assertThat(ModifyDataStreamsAction.NAME, equalTo("indices:admin/data_stream/modify"));
        assertThat(ModifyDataStreamsAction.INSTANCE.name(), equalTo(ModifyDataStreamsAction.NAME));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new AcknowledgedResponse(true).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                AcknowledgedResponse response = ModifyDataStreamsAction.INSTANCE.getResponseReader().read(in);
                assertTrue(response.isAcknowledged());
            }
        }
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
