/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DataStreamActionTests extends OpenSearchTestCase {

    private static final String DS = "logs-foo";
    private static final String INDEX = ".ds-logs-foo-000001";

    // ---------------------------------------------------------------- Type enum

    public void testTypeValue() {
        assertThat(DataStreamAction.Type.ADD_BACKING_INDEX.value(), equalTo((byte) 0));
        assertThat(DataStreamAction.Type.REMOVE_BACKING_INDEX.value(), equalTo((byte) 1));
    }

    public void testTypeFieldName() {
        assertThat(DataStreamAction.Type.ADD_BACKING_INDEX.fieldName(), equalTo("add_backing_index"));
        assertThat(DataStreamAction.Type.REMOVE_BACKING_INDEX.fieldName(), equalTo("remove_backing_index"));
    }

    public void testTypeFromValueValid() {
        assertThat(DataStreamAction.Type.fromValue((byte) 0), equalTo(DataStreamAction.Type.ADD_BACKING_INDEX));
        assertThat(DataStreamAction.Type.fromValue((byte) 1), equalTo(DataStreamAction.Type.REMOVE_BACKING_INDEX));
    }

    public void testTypeFromValueInvalidThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataStreamAction.Type.fromValue((byte) 2));
        assertThat(e.getMessage(), equalTo("no data stream action type for [2]"));

        IllegalArgumentException negative = expectThrows(IllegalArgumentException.class, () -> DataStreamAction.Type.fromValue((byte) -1));
        assertThat(negative.getMessage(), equalTo("no data stream action type for [-1]"));
    }

    public void testTypeValuesAreRoundTrippableThroughFromValue() {
        for (DataStreamAction.Type type : DataStreamAction.Type.values()) {
            assertThat(DataStreamAction.Type.fromValue(type.value()), equalTo(type));
        }
    }

    // ---------------------------------------------------------------- constructor null checks

    public void testConstructorRejectsNullType() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new DataStreamAction(null, DS, INDEX));
        assertThat(e.getMessage(), equalTo("[type] is required"));
    }

    public void testConstructorRejectsNullDataStream() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new DataStreamAction(DataStreamAction.Type.ADD_BACKING_INDEX, null, INDEX)
        );
        assertThat(e.getMessage(), equalTo("[data_stream] is required"));
    }

    public void testConstructorRejectsNullIndex() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new DataStreamAction(DataStreamAction.Type.REMOVE_BACKING_INDEX, DS, null)
        );
        assertThat(e.getMessage(), equalTo("[index] is required"));
    }

    // ---------------------------------------------------------------- accessors + factories

    public void testAccessors() {
        DataStreamAction action = new DataStreamAction(DataStreamAction.Type.REMOVE_BACKING_INDEX, DS, INDEX);
        assertThat(action.type(), equalTo(DataStreamAction.Type.REMOVE_BACKING_INDEX));
        assertThat(action.dataStream(), equalTo(DS));
        assertThat(action.index(), equalTo(INDEX));
    }

    public void testAddBackingIndexFactory() {
        DataStreamAction action = DataStreamAction.addBackingIndex(DS, INDEX);
        assertThat(action.type(), equalTo(DataStreamAction.Type.ADD_BACKING_INDEX));
        assertThat(action.dataStream(), equalTo(DS));
        assertThat(action.index(), equalTo(INDEX));
        assertThat(action, equalTo(new DataStreamAction(DataStreamAction.Type.ADD_BACKING_INDEX, DS, INDEX)));
    }

    public void testRemoveBackingIndexFactory() {
        DataStreamAction action = DataStreamAction.removeBackingIndex(DS, INDEX);
        assertThat(action.type(), equalTo(DataStreamAction.Type.REMOVE_BACKING_INDEX));
        assertThat(action.dataStream(), equalTo(DS));
        assertThat(action.index(), equalTo(INDEX));
        assertThat(action, equalTo(new DataStreamAction(DataStreamAction.Type.REMOVE_BACKING_INDEX, DS, INDEX)));
    }

    // ---------------------------------------------------------------- wire serialization

    public void testSerializationRoundTripAdd() throws IOException {
        assertSerializationRoundTrip(DataStreamAction.addBackingIndex(DS, INDEX));
    }

    public void testSerializationRoundTripRemove() throws IOException {
        assertSerializationRoundTrip(DataStreamAction.removeBackingIndex(DS, INDEX));
    }

    public void testSerializationRoundTripRandom() throws IOException {
        for (DataStreamAction.Type type : DataStreamAction.Type.values()) {
            String ds = randomAlphaOfLength(8);
            String index = randomAlphaOfLength(12);
            assertSerializationRoundTrip(new DataStreamAction(type, ds, index));
        }
    }

    public void testStreamInputRejectsUnknownTypeByte() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte((byte) 7);
            out.writeString(DS);
            out.writeString(INDEX);
            try (StreamInput in = out.bytes().streamInput()) {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new DataStreamAction(in));
                assertThat(e.getMessage(), equalTo("no data stream action type for [7]"));
            }
        }
    }

    private void assertSerializationRoundTrip(DataStreamAction original) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                DataStreamAction deserialized = new DataStreamAction(in);
                assertThat(deserialized.type(), equalTo(original.type()));
                assertThat(deserialized.dataStream(), equalTo(original.dataStream()));
                assertThat(deserialized.index(), equalTo(original.index()));
                assertThat(deserialized, equalTo(original));
                assertThat(deserialized.hashCode(), equalTo(original.hashCode()));
            }
        }
    }

    // ---------------------------------------------------------------- toXContent

    public void testToXContentAdd() throws IOException {
        assertThat(
            toJson(DataStreamAction.addBackingIndex(DS, INDEX)),
            equalTo("{\"add_backing_index\":{\"data_stream\":\"" + DS + "\",\"index\":\"" + INDEX + "\"}}")
        );
    }

    public void testToXContentRemove() throws IOException {
        assertThat(
            toJson(DataStreamAction.removeBackingIndex(DS, INDEX)),
            equalTo("{\"remove_backing_index\":{\"data_stream\":\"" + DS + "\",\"index\":\"" + INDEX + "\"}}")
        );
    }

    private String toJson(DataStreamAction action) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            action.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    // ---------------------------------------------------------------- fromXContent

    public void testFromXContentAdd() throws IOException {
        String json = "{\"add_backing_index\":{\"data_stream\":\"" + DS + "\",\"index\":\"" + INDEX + "\"}}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            DataStreamAction action = DataStreamAction.fromXContent(parser);
            assertThat(action, equalTo(DataStreamAction.addBackingIndex(DS, INDEX)));
        }
    }

    public void testFromXContentRemove() throws IOException {
        String json = "{\"remove_backing_index\":{\"data_stream\":\"" + DS + "\",\"index\":\"" + INDEX + "\"}}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            DataStreamAction action = DataStreamAction.fromXContent(parser);
            assertThat(action, equalTo(DataStreamAction.removeBackingIndex(DS, INDEX)));
        }
    }

    public void testXContentRoundTrip() throws IOException {
        for (DataStreamAction.Type type : DataStreamAction.Type.values()) {
            DataStreamAction original = new DataStreamAction(type, randomAlphaOfLength(6), randomAlphaOfLength(10));
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), toJson(original))) {
                assertThat(DataStreamAction.fromXContent(parser), equalTo(original));
            }
        }
    }

    public void testFromXContentUnknownActionNameThrows() throws IOException {
        String json = "{\"rotate_backing_index\":{\"data_stream\":\"" + DS + "\",\"index\":\"" + INDEX + "\"}}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(e.getMessage(), containsString("rotate_backing_index"));
        }
    }

    public void testFromXContentMissingIndexThrows() throws IOException {
        String json = "{\"add_backing_index\":{\"data_stream\":\"" + DS + "\"}}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(e.getMessage(), containsString("index"));
        }
    }

    public void testFromXContentMissingDataStreamThrows() throws IOException {
        String json = "{\"remove_backing_index\":{\"index\":\"" + INDEX + "\"}}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(e.getMessage(), containsString("data_stream"));
        }
    }

    public void testFromXContentUnknownInnerFieldThrows() throws IOException {
        String json = "{\"add_backing_index\":{\"data_stream\":\"" + DS + "\",\"index\":\"" + INDEX + "\",\"bogus\":\"value\"}}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(e.getMessage(), containsString("add_backing_index"));
            assertNotNull(e.getCause());
            assertThat(e.getCause().getMessage(), containsString("bogus"));
        }
    }

    public void testFromXContentNotAnObjectThrows() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "[]")) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(e.getMessage(), containsString("Expected START_OBJECT"));
        }
    }

    public void testFromXContentEmptyObjectYieldsNoAction() throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{}")) {
            assertNull(DataStreamAction.fromXContent(parser));
        }
    }

    public void testFromXContentWrongInnerTypeThrows() throws IOException {
        String json = "{\"add_backing_index\":\"not-an-object\"}";
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            expectThrows(XContentParseException.class, () -> DataStreamAction.fromXContent(parser));
        }
    }

    // ---------------------------------------------------------------- equals / hashCode / toString

    public void testEqualsSameInstance() {
        DataStreamAction action = DataStreamAction.addBackingIndex(DS, INDEX);
        assertTrue(action.equals(action));
    }

    public void testEqualsNull() {
        DataStreamAction action = DataStreamAction.addBackingIndex(DS, INDEX);
        assertFalse(action.equals(null));
    }

    public void testEqualsDifferentClass() {
        DataStreamAction action = DataStreamAction.addBackingIndex(DS, INDEX);
        assertFalse(action.equals("add_backing_index"));
        assertFalse(action.equals(new Object()));
    }

    public void testEqualsEquivalentInstances() {
        DataStreamAction one = DataStreamAction.addBackingIndex(DS, INDEX);
        DataStreamAction two = new DataStreamAction(DataStreamAction.Type.ADD_BACKING_INDEX, new String(DS), new String(INDEX));
        assertNotSame(one, two);
        assertTrue(one.equals(two));
        assertTrue(two.equals(one));
        assertThat(one.hashCode(), equalTo(two.hashCode()));
    }

    public void testNotEqualsOnType() {
        DataStreamAction add = DataStreamAction.addBackingIndex(DS, INDEX);
        DataStreamAction remove = DataStreamAction.removeBackingIndex(DS, INDEX);
        assertFalse(add.equals(remove));
        assertThat(add.hashCode(), not(equalTo(remove.hashCode())));
    }

    public void testNotEqualsOnDataStream() {
        DataStreamAction one = DataStreamAction.addBackingIndex(DS, INDEX);
        DataStreamAction two = DataStreamAction.addBackingIndex("logs-other", INDEX);
        assertFalse(one.equals(two));
        assertThat(one.hashCode(), not(equalTo(two.hashCode())));
    }

    public void testNotEqualsOnIndex() {
        DataStreamAction one = DataStreamAction.addBackingIndex(DS, INDEX);
        DataStreamAction two = DataStreamAction.addBackingIndex(DS, ".ds-logs-foo-000002");
        assertFalse(one.equals(two));
        assertThat(one.hashCode(), not(equalTo(two.hashCode())));
    }

    public void testHashCodeIsStable() {
        DataStreamAction action = DataStreamAction.removeBackingIndex(DS, INDEX);
        assertThat(action.hashCode(), equalTo(action.hashCode()));
    }

    public void testToStringAdd() {
        assertThat(
            DataStreamAction.addBackingIndex(DS, INDEX).toString(),
            equalTo("DataStreamAction[type=ADD_BACKING_INDEX, dataStream=" + DS + ", index=" + INDEX + "]")
        );
    }

    public void testToStringRemove() {
        assertThat(
            DataStreamAction.removeBackingIndex(DS, INDEX).toString(),
            equalTo("DataStreamAction[type=REMOVE_BACKING_INDEX, dataStream=" + DS + ", index=" + INDEX + "]")
        );
    }
}
