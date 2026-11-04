/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.alias;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/** The enforcement mode on {@link Alias}, the model used by create-index requests and legacy templates. */
public class AliasEnforcementTests extends OpenSearchTestCase {

    public void testParsesTheMode() throws IOException {
        // Alias ignores string fields it does not know, so before it knew this one a create-index request silently dropped it.
        String json = "{\"scoped\":{\"filter\":{\"term\":{\"f\":\"v\"}},\"enforcement\":\"pre_filter\"}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // the enclosing object
            parser.nextToken(); // the alias name, where fromXContent starts
            assertEquals("pre_filter", Alias.fromXContent(parser).enforcement());
        }
    }

    public void testRejectsAnUnknownMode() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Alias("scoped").enforcement("pre_filer"));
        assertThat(e.getMessage(), containsString("unknown alias enforcement [pre_filer]"));
    }

    public void testWritesTheMode() throws IOException {
        Alias alias = new Alias("scoped").filter("{\"term\":{\"f\":\"v\"}}").enforcement("pre_filter");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        alias.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertThat(builder.toString(), containsString("\"enforcement\":\"pre_filter\""));
    }

    public void testRoundTripsAndIsDroppedForAnOlderNode() throws IOException {
        Alias alias = new Alias("scoped").filter("{\"term\":{\"f\":\"v\"}}").enforcement("pre_filter");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            alias.writeTo(out);
            assertEquals("pre_filter", new Alias(out.bytes().streamInput()).enforcement());
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_3_9_0);
            alias.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_3_9_0);
            Alias read = new Alias(in);
            assertNull(read.enforcement());
            assertEquals("the stream must be fully consumed", 0, in.available());
        }
    }
}
