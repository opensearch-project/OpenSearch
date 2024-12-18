package org.opensearch.accesscontrol.resources;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreatedByTests extends OpenSearchTestCase {

    public void testCreatedByConstructorWithValidUser() {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(expectedUser);

        MatcherAssert.assertThat(expectedUser, is(equalTo(createdBy.getUser())));
    }

    public void testCreatedByFromStreamInput() throws IOException {
        String expectedUser = "testUser";

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(expectedUser);

            StreamInput in = out.bytes().streamInput();

            CreatedBy createdBy = new CreatedBy(in);

            MatcherAssert.assertThat(expectedUser, is(equalTo(createdBy.getUser())));
        }
    }

    public void testCreatedByWithEmptyStreamInput() throws IOException {

        try (StreamInput mockStreamInput = mock(StreamInput.class)) {
            when(mockStreamInput.readString()).thenThrow(new IOException("EOF"));

            assertThrows(IOException.class, () -> new CreatedBy(mockStreamInput));
        }
    }

    public void testCreatedByWithEmptyUser() {

        CreatedBy createdBy = new CreatedBy("");
        MatcherAssert.assertThat("", equalTo(createdBy.getUser()));
    }

    public void testCreatedByWithIOException() throws IOException {

        try (StreamInput mockStreamInput = mock(StreamInput.class)) {
            when(mockStreamInput.readString()).thenThrow(new IOException("Test IOException"));

            assertThrows(IOException.class, () -> new CreatedBy(mockStreamInput));
        }
    }

    public void testCreatedByWithLongUsername() {
        String longUsername = "a".repeat(10000);
        CreatedBy createdBy = new CreatedBy(longUsername);
        MatcherAssert.assertThat(longUsername, equalTo(createdBy.getUser()));
    }

    public void testCreatedByWithUnicodeCharacters() {
        String unicodeUsername = "用户こんにちは";
        CreatedBy createdBy = new CreatedBy(unicodeUsername);
        MatcherAssert.assertThat(unicodeUsername, equalTo(createdBy.getUser()));
    }

    public void testFromXContentThrowsExceptionWhenUserFieldIsMissing() throws IOException {
        String json = "{}";
        IllegalArgumentException exception;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {

            exception = assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }

        MatcherAssert.assertThat("user field is required", equalTo(exception.getMessage()));
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

    public void testFromXContentWithMissingUser() throws IOException {
        String json = "{}";
        IllegalArgumentException exception;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to the start object token

            exception = assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }

        MatcherAssert.assertThat("user field is required", equalTo(exception.getMessage()));
    }

    public void testFromXContentWithMissingUserField() throws IOException {
        String jsonWithoutUser = "{\"someOtherField\": \"value\"}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonWithoutUser)) {

            assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }
    }

    public void testFromXContentWithNullUserValue() throws IOException {
        String jsonWithNullUser = "{\"user\": null}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, jsonWithNullUser)) {

            assertThrows(IllegalArgumentException.class, () -> CreatedBy.fromXContent(parser));
        }
    }

    public void testFromXContentWithValidUser() throws IOException {
        String json = "{\"user\":\"testUser\"}";
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, json);

        CreatedBy createdBy = CreatedBy.fromXContent(parser);

        assertNotNull(createdBy);
        MatcherAssert.assertThat("testUser", equalTo(createdBy.getUser()));
    }

    public void testGetUserReturnsCorrectValue() {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(expectedUser);

        String actualUser = createdBy.getUser();

        MatcherAssert.assertThat(expectedUser, equalTo(actualUser));
    }

    public void testGetUserWithNullString() {

        CreatedBy createdBy = new CreatedBy((String) null);
        assertNull("getUser should return null when initialized with null", createdBy.getUser());
    }

    public void testGetWriteableNameReturnsCorrectString() {
        CreatedBy createdBy = new CreatedBy("testUser");
        MatcherAssert.assertThat("created_by", equalTo(createdBy.getWriteableName()));
    }

    public void testSetUserWithEmptyString() {
        CreatedBy createdBy = new CreatedBy("initialUser");
        createdBy.setUser("");
        MatcherAssert.assertThat("", equalTo(createdBy.getUser()));
    }

    public void testToStringWithEmptyUser() {
        CreatedBy createdBy = new CreatedBy("");
        String result = createdBy.toString();
        MatcherAssert.assertThat("CreatedBy {user=''}", equalTo(result));
    }

    public void testToStringWithNullUser() {
        CreatedBy createdBy = new CreatedBy((String) null);
        String result = createdBy.toString();
        MatcherAssert.assertThat("CreatedBy {user='null'}", equalTo(result));
    }

    public void testToStringWithLongUserName() {

        String longUserName = "a".repeat(1000);
        CreatedBy createdBy = new CreatedBy(longUserName);
        String result = createdBy.toString();
        assertTrue(result.startsWith("CreatedBy {user='"));
        assertTrue(result.endsWith("'}"));
        MatcherAssert.assertThat(1019, equalTo(result.length()));
    }

    public void testToXContentWithEmptyUser() throws IOException {
        CreatedBy createdBy = new CreatedBy("");
        XContentBuilder builder = JsonXContent.contentBuilder();

        createdBy.toXContent(builder, null);
        String result = builder.toString();
        MatcherAssert.assertThat("{\"user\":\"\"}", equalTo(result));
    }

    public void testWriteToWithExceptionInStreamOutput() throws IOException {
        CreatedBy createdBy = new CreatedBy("user1");
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
        CreatedBy createdBy = new CreatedBy(longUserName);
        BytesStreamOutput out = new BytesStreamOutput();
        createdBy.writeTo(out);
        assertTrue(out.size() > 65536);
    }

    public void test_createdByToStringReturnsCorrectFormat() {
        String testUser = "testUser";
        CreatedBy createdBy = new CreatedBy(testUser);

        String expected = "CreatedBy {user='" + testUser + "'}";
        String actual = createdBy.toString();

        MatcherAssert.assertThat(expected, equalTo(actual));
    }

    public void test_fromXContent_missingUserField() throws IOException {
        String json = "{}";
        IllegalArgumentException exception;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken(); // Move to the start object token

            exception = assertThrows(IllegalArgumentException.class, () -> { CreatedBy.fromXContent(parser); });
        }

        MatcherAssert.assertThat("user field is required", equalTo(exception.getMessage()));
    }

    public void test_toXContent_serializesCorrectly() throws IOException {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(expectedUser);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        createdBy.toXContent(builder, null);

        String expectedJson = "{\"user\":\"testUser\"}";
        MatcherAssert.assertThat(expectedJson, equalTo(builder.toString()));
    }

    public void test_writeTo_writesUserCorrectly() throws IOException {
        String expectedUser = "testUser";
        CreatedBy createdBy = new CreatedBy(expectedUser);

        BytesStreamOutput out = new BytesStreamOutput();
        createdBy.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        String actualUser = in.readString();

        MatcherAssert.assertThat(expectedUser, equalTo(actualUser));
    }

}
