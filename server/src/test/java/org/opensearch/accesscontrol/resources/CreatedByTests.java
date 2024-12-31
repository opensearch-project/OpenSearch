/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreatedByTests extends OpenSearchTestCase {

    private static final String CREATOR_TYPE = "user";

    public void testCreatedByConstructorWithValidUser() {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, expectedUser);

        MatcherAssert.assertThat(expectedUser, is(equalTo(createdBy.getCreator())));
    }

    public void testCreatedByFromStreamInput() throws IOException {
        String expectedUser = "testUser";

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(CREATOR_TYPE);
            out.writeString(expectedUser);

            StreamInput in = out.bytes().streamInput();

            CreatedBy createdBy = new CreatedBy(in);

            MatcherAssert.assertThat(expectedUser, is(equalTo(createdBy.getCreator())));
        }
    }

    public void testCreatedByWithEmptyStreamInput() throws IOException {

        try (StreamInput mockStreamInput = mock(StreamInput.class)) {
            when(mockStreamInput.readString()).thenThrow(new IOException("EOF"));

            assertThrows(IOException.class, () -> new CreatedBy(mockStreamInput));
        }
    }

    public void testCreatedByWithEmptyUser() {

        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, "");
        MatcherAssert.assertThat("", equalTo(createdBy.getCreator()));
    }

    public void testCreatedByWithIOException() throws IOException {

        try (StreamInput mockStreamInput = mock(StreamInput.class)) {
            when(mockStreamInput.readString()).thenThrow(new IOException("Test IOException"));

            assertThrows(IOException.class, () -> new CreatedBy(mockStreamInput));
        }
    }

    public void testCreatedByWithLongUsername() {
        String longUsername = "a".repeat(10000);
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, longUsername);
        MatcherAssert.assertThat(longUsername, equalTo(createdBy.getCreator()));
    }

    public void testCreatedByWithUnicodeCharacters() {
        String unicodeUsername = "用户こんにちは";
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, unicodeUsername);
        MatcherAssert.assertThat(unicodeUsername, equalTo(createdBy.getCreator()));
    }

    public void testFromXContentThrowsExceptionWhenUserFieldIsMissing() throws IOException {
        String emptyJson = "{}";
        IllegalArgumentException exception;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, emptyJson)) {

            exception = assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }

        MatcherAssert.assertThat("null is required", equalTo(exception.getMessage()));
    }

    public void testFromXContentWithEmptyInput() throws IOException {
        String emptyJson = "{}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, emptyJson)) {

            assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }
    }

    public void testFromXContentWithExtraFields() throws IOException {
        String jsonWithExtraFields = "{\"user\": \"testUser\", \"extraField\": \"value\"}";
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonWithExtraFields);

        CreatedBy.fromXContent(parser);
    }

    public void testFromXContentWithIncorrectFieldType() throws IOException {
        String jsonWithIncorrectType = "{\"user\": 12345}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonWithIncorrectType)) {

            assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }
    }

    public void testFromXContentWithEmptyUser() throws IOException {
        String emptyJson = "{\"" + CREATOR_TYPE + "\": \"\" }";
        CreatedBy createdBy;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, emptyJson)) {
            parser.nextToken();

            createdBy = CreatedBy.fromXContent(parser);
        }

        MatcherAssert.assertThat(CREATOR_TYPE, equalTo(createdBy.getCreatorType()));
        MatcherAssert.assertThat("", equalTo(createdBy.getCreator()));
    }

    public void testFromXContentWithNullUserValue() throws IOException {
        String jsonWithNullUser = "{\"user\": null}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonWithNullUser)) {

            assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }
    }

    public void testFromXContentWithValidUser() throws IOException {
        String json = "{\"user\":\"testUser\"}";
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);

        CreatedBy createdBy = CreatedBy.fromXContent(parser);

        MatcherAssert.assertThat(createdBy, notNullValue());
        MatcherAssert.assertThat("testUser", equalTo(createdBy.getCreator()));
    }

    public void testGetCreatorReturnsCorrectValue() {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, expectedUser);

        String actualUser = createdBy.getCreator();

        MatcherAssert.assertThat(expectedUser, equalTo(actualUser));
    }

    public void testGetCreatorWithNullString() {

        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, null);
        MatcherAssert.assertThat(createdBy.getCreator(), nullValue());
    }

    public void testGetWriteableNameReturnsCorrectString() {
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, "testUser");
        MatcherAssert.assertThat("created_by", equalTo(createdBy.getWriteableName()));
    }

    public void testToStringWithEmptyUser() {
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, "");
        String result = createdBy.toString();
        MatcherAssert.assertThat("CreatedBy {user=''}", equalTo(result));
    }

    public void testToStringWithNullUser() {
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, (String) null);
        String result = createdBy.toString();
        MatcherAssert.assertThat("CreatedBy {user='null'}", equalTo(result));
    }

    public void testToStringWithLongUserName() {

        String longUserName = "a".repeat(1000);
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, longUserName);
        String result = createdBy.toString();
        MatcherAssert.assertThat(result.startsWith("CreatedBy {user='"), is(true));
        MatcherAssert.assertThat(result.endsWith("'}"), is(true));
        MatcherAssert.assertThat(1019, equalTo(result.length()));
    }

    public void testToXContentWithEmptyUser() throws IOException {
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, "");
        XContentBuilder builder = JsonXContent.contentBuilder();

        createdBy.toXContent(builder, null);
        String result = builder.toString();
        MatcherAssert.assertThat("{\"user\":\"\"}", equalTo(result));
    }

    public void testWriteToWithExceptionInStreamOutput() throws IOException {
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, "user1");
        try (StreamOutput failingOutput = new StreamOutput() {
            @Override
            public void writeByte(byte b) throws IOException {
                throw new IOException("Simulated IO exception");
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                throw new IOException("Simulated IO exception");
            }

            @Override
            public void flush() throws IOException {

            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public void reset() throws IOException {

            }
        }) {

            assertThrows(IOException.class, () -> createdBy.writeTo(failingOutput));
        }
    }

    public void testWriteToWithLongUserName() throws IOException {
        String longUserName = "a".repeat(65536);
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, longUserName);
        BytesStreamOutput out = new BytesStreamOutput();
        createdBy.writeTo(out);
        MatcherAssert.assertThat(out.size(), greaterThan(65536));
    }

    public void test_createdByToStringReturnsCorrectFormat() {
        String testUser = "testUser";
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, testUser);

        String expected = "CreatedBy {user='" + testUser + "'}";
        String actual = createdBy.toString();

        MatcherAssert.assertThat(expected, equalTo(actual));
    }

    public void test_toXContent_serializesCorrectly() throws IOException {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, expectedUser);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        createdBy.toXContent(builder, null);

        String expectedJson = "{\"user\":\"testUser\"}";
        MatcherAssert.assertThat(expectedJson, equalTo(builder.toString()));
    }

    public void test_writeTo_writesUserCorrectly() throws IOException {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(CREATOR_TYPE, expectedUser);

        BytesStreamOutput out = new BytesStreamOutput();
        createdBy.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.readString();
        String actualUser = in.readString();

        MatcherAssert.assertThat(expectedUser, equalTo(actualUser));
    }

}
