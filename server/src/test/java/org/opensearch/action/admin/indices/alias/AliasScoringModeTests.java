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
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/** The scoring mode on {@link Alias}, the model used by create-index requests and legacy templates. */
public class AliasScoringModeTests extends OpenSearchTestCase {

    public void testParsesTheMode() throws IOException {
        // Alias ignores string fields it does not know, so before it knew this one a create-index request silently dropped it.
        String json = "{\"scoped\":{\"filter\":{\"term\":{\"f\":\"v\"}},\"scoring_mode\":\"no_stats\"}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // the enclosing object
            parser.nextToken(); // the alias name, where fromXContent starts
            assertEquals("no_stats", Alias.fromXContent(parser).scoringMode());
        }
    }

    public void testRejectsAnUnknownMode() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Alias("scoped").scoringMode("no_stat"));
        assertThat(e.getMessage(), containsString("unknown alias scoring mode [no_stat]"));
    }

    public void testWritesTheMode() throws IOException {
        Alias alias = new Alias("scoped").filter("{\"term\":{\"f\":\"v\"}}").scoringMode("no_stats");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        alias.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertThat(builder.toString(), containsString("\"scoring_mode\":\"no_stats\""));
    }

    public void testRoundTripsAndIsRejectedForAnOlderNode() throws IOException {
        Alias alias = new Alias("scoped").filter("{\"term\":{\"f\":\"v\"}}").scoringMode("no_stats");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            alias.writeTo(out);
            assertEquals("no_stats", new Alias(out.bytes().streamInput()).scoringMode());
        }
        final Version older = VersionUtils.getPreviousVersion(AliasFilter.SCORING_MODE_VERSION);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(older);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> alias.writeTo(out));
            assertThat(e.getMessage(), containsString("[scoring_mode] on alias [scoped]"));
        }
        Alias plain = new Alias("scoped").filter("{\"term\":{\"f\":\"v\"}}");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(older);
            plain.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(older);
            assertNull(new Alias(in).scoringMode());
            assertEquals("the stream must be fully consumed", 0, in.available());
        }
    }
}
