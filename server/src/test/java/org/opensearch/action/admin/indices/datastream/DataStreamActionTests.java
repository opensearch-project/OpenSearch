/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamActionTests extends OpenSearchTestCase {

    public void testAddBackingIndexXContentRoundTrip() throws IOException {
        assertXContentRoundTrip(DataStreamAction.addBackingIndex("my-data-stream", "my-index"));
    }

    public void testRemoveBackingIndexXContentRoundTrip() throws IOException {
        assertXContentRoundTrip(DataStreamAction.removeBackingIndex("my-data-stream", "my-index"));
    }

    private void assertXContentRoundTrip(DataStreamAction action) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        action.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, builder.toString())) {
            DataStreamAction parsed = DataStreamAction.fromXContent(parser);
            assertThat(parsed, equalTo(action));
        }
    }

    public void testRejectsEntryWithoutAction() throws IOException {
        String json = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(causeChainMessages(e), containsString("no data stream operation declared"));
        }
    }

    public void testRejectsEntryWithMultipleActions() throws IOException {
        String json = "{ \"add_backing_index\": { \"data_stream\": \"ds\", \"index\": \"i1\" }, "
            + "\"remove_backing_index\": { \"data_stream\": \"ds\", \"index\": \"i2\" } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(causeChainMessages(e), containsString("too many data stream operations declared"));
        }
    }

    public void testRejectsActionWithoutIndex() throws IOException {
        String json = "{ \"add_backing_index\": { \"data_stream\": \"ds\" } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(causeChainMessages(e), containsString("index"));
        }
    }

    public void testRejectsActionWithoutDataStream() throws IOException {
        String json = "{ \"remove_backing_index\": { \"index\": \"i1\" } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataStreamAction.fromXContent(parser));
            assertThat(causeChainMessages(e), containsString("data_stream"));
        }
    }

    // ConstructingObjectParser wraps validation errors thrown from its builder lambda, so the custom message
    // surfaces somewhere in the cause chain rather than on the top-level exception.
    private static String causeChainMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append('\n');
        }
        return sb.toString();
    }
}
