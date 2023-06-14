/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.histogram;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.ZoneOffset;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.equalTo;

public class LongBoundsTests extends OpenSearchTestCase {
    /**
     * Construct a random {@link LongBounds}.
     */
    public static LongBounds randomExtendedBounds() {
        LongBounds bounds = randomParsedExtendedBounds();
        if (randomBoolean()) {
            bounds = unparsed(bounds);
        }
        return bounds;
    }

    /**
     * Construct a random {@link LongBounds} in pre-parsed form.
     */
    public static LongBounds randomParsedExtendedBounds() {
        long maxDateValue = 253402300799999L; // end of year 9999
        long minDateValue = -377705116800000L; // beginning of year -9999
        if (randomBoolean()) {
            // Construct with one missing bound
            if (randomBoolean()) {
                return new LongBounds(null, maxDateValue);
            }
            return new LongBounds(minDateValue, null);
        }
        long a = randomLongBetween(minDateValue, maxDateValue);
        long b;
        do {
            b = randomLongBetween(minDateValue, maxDateValue);
        } while (a == b);
        long min = min(a, b);
        long max = max(a, b);
        return new LongBounds(min, max);
    }

    /**
     * Convert an extended bounds in parsed for into one in unparsed form.
     */
    public static LongBounds unparsed(LongBounds template) {
        // It'd probably be better to randomize the formatter
        DateFormatter formatter = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
        String minAsStr = template.getMin() == null ? null : formatter.formatMillis(template.getMin());
        String maxAsStr = template.getMax() == null ? null : formatter.formatMillis(template.getMax());
        return new LongBounds(minAsStr, maxAsStr);
    }

    public void testParseAndValidate() {
        long now = randomLong();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        QueryShardContext qsc = new QueryShardContext(
            0,
            new IndexSettings(IndexMetadata.builder("foo").settings(indexSettings).build(), indexSettings),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            () -> now,
            null,
            null,
            () -> true,
            null
        );
        DateFormatter formatter = DateFormatter.forPattern("date_optional_time");
        DocValueFormat format = new DocValueFormat.DateTime(formatter, ZoneOffset.UTC, DateFieldMapper.Resolution.MILLISECONDS);

        LongBounds expected = randomParsedExtendedBounds();
        LongBounds parsed = unparsed(expected).parseAndValidate("test", "extended_bounds", qsc, format);
        // parsed won't *equal* expected because equal includes the String parts
        assertEquals(expected.getMin(), parsed.getMin());
        assertEquals(expected.getMax(), parsed.getMax());

        parsed = new LongBounds("now", null).parseAndValidate("test", "extended_bounds", qsc, format);
        assertEquals(now, (long) parsed.getMin());
        assertNull(parsed.getMax());

        parsed = new LongBounds(null, "now").parseAndValidate("test", "extended_bounds", qsc, format);
        assertNull(parsed.getMin());
        assertEquals(now, (long) parsed.getMax());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new LongBounds(100L, 90L).parseAndValidate("test", "extended_bounds", qsc, format)
        );
        assertEquals(
            "[extended_bounds.min][100] cannot be greater than [extended_bounds.max][90] for histogram aggregation [test]",
            e.getMessage()
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> unparsed(new LongBounds(100L, 90L)).parseAndValidate("test", "extended_bounds", qsc, format)
        );
        assertEquals(
            "[extended_bounds.min][100] cannot be greater than [extended_bounds.max][90] for histogram aggregation [test]",
            e.getMessage()
        );
    }

    public void testTransportRoundTrip() throws IOException {
        LongBounds orig = randomExtendedBounds();

        BytesReference origBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            orig.writeTo(out);
            origBytes = out.bytes();
        }

        LongBounds read;
        try (StreamInput in = origBytes.streamInput()) {
            read = new LongBounds(in);
            assertEquals("read fully", 0, in.available());
        }
        assertEquals(orig, read);

        BytesReference readBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            read.writeTo(out);
            readBytes = out.bytes();
        }

        assertEquals(origBytes, readBytes);
    }

    public void testXContentRoundTrip() throws Exception {
        LongBounds orig = randomExtendedBounds();

        try (XContentBuilder out = JsonXContent.contentBuilder()) {
            out.startObject();
            orig.toXContent(out, ToXContent.EMPTY_PARAMS);
            out.endObject();

            try (XContentParser in = createParser(JsonXContent.jsonXContent, BytesReference.bytes(out))) {
                XContentParser.Token token = in.currentToken();
                assertNull(token);

                token = in.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                LongBounds read = LongBounds.PARSER.apply(in, null);
                assertEquals(orig, read);
            } catch (Exception e) {
                throw new Exception("Error parsing [" + BytesReference.bytes(out).utf8ToString() + "]", e);
            }
        }
    }
}
