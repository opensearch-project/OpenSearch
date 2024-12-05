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

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.AbstractSortedSetDocValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude.OrdinalsFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;

public class IncludeExcludeTests extends OpenSearchTestCase {

    public void testEmptyTermsWithOrds() throws IOException {
        IncludeExclude inexcl = new IncludeExclude(new TreeSet<>(Collections.singleton(new BytesRef("foo"))), null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());

        inexcl = new IncludeExclude(null, new TreeSet<>(Collections.singleton(new BytesRef("foo"))));
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(DocValues.emptySortedSet());
        assertEquals(0, acceptedOrds.length());
    }

    public void testSingleTermWithOrds() throws IOException {
        SortedSetDocValues ords = new AbstractSortedSetDocValues() {

            boolean consumed = true;

            @Override
            public boolean advanceExact(int docID) {
                consumed = false;
                return true;
            }

            @Override
            public long nextOrd() {
                if (consumed) {
                    return SortedSetDocValues.NO_MORE_ORDS;
                } else {
                    consumed = true;
                    return 0;
                }
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                assertEquals(0, ord);
                return new BytesRef("foo");
            }

            @Override
            public long getValueCount() {
                return 1;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
        IncludeExclude inexcl = new IncludeExclude(new TreeSet<>(Collections.singleton(new BytesRef("foo"))), null);
        OrdinalsFilter filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        LongBitSet acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertTrue(acceptedOrds.get(0));

        inexcl = new IncludeExclude(new TreeSet<>(Collections.singleton(new BytesRef("bar"))), null);
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
            new TreeSet<>(Collections.singleton(new BytesRef("foo"))),
            new TreeSet<>(Collections.singleton(new BytesRef("foo")))
        );
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));

        inexcl = new IncludeExclude(
            null, // means everything included
            new TreeSet<>(Collections.singleton(new BytesRef("foo")))
        );
        filter = inexcl.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = filter.acceptedGlobalOrdinals(ords);
        assertEquals(1, acceptedOrds.length());
        assertFalse(acceptedOrds.get(0));
    }

    public void testPartitionedEquals() throws IOException {
        IncludeExclude serialized = serialize(new IncludeExclude(3, 20), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isRegexBased());
        assertTrue(serialized.isPartitionBased());

        IncludeExclude same = new IncludeExclude(3, 20);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude differentParam1 = new IncludeExclude(4, 20);
        assertFalse(serialized.equals(differentParam1));
        assertTrue(serialized.hashCode() != differentParam1.hashCode());

        IncludeExclude differentParam2 = new IncludeExclude(3, 21);
        assertFalse(serialized.equals(differentParam2));
        assertTrue(serialized.hashCode() != differentParam2.hashCode());
    }

    public void testExactIncludeValuesEquals() throws IOException {
        String[] incValues = { "a", "b" };
        String[] differentIncValues = { "a", "c" };
        IncludeExclude serialized = serialize(new IncludeExclude(incValues, null), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertFalse(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incValues, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(differentIncValues, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testExactExcludeValuesEquals() throws IOException {
        String[] excValues = { "a", "b" };
        String[] differentExcValues = { "a", "c" };
        IncludeExclude serialized = serialize(new IncludeExclude(null, excValues), IncludeExclude.EXCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertFalse(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, excValues);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, differentExcValues);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexInclude() throws IOException {
        String incRegex = "foo.*";
        String differentRegex = "bar.*";
        IncludeExclude serialized = serialize(new IncludeExclude(incRegex, null), IncludeExclude.INCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, null);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(differentRegex, null);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    public void testRegexExclude() throws IOException {
        String excRegex = "foo.*";
        String differentRegex = "bar.*";
        IncludeExclude serialized = serialize(new IncludeExclude(null, excRegex), IncludeExclude.EXCLUDE_FIELD);
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(null, excRegex);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(null, differentRegex);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    // Serializes/deserializes an IncludeExclude statement with a single clause
    private IncludeExclude serialize(IncludeExclude incExc, ParseField field) throws IOException {
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        incExc.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(builder)) {
            XContentParser.Token token = parser.nextToken();
            assertEquals(token, XContentParser.Token.START_OBJECT);
            token = parser.nextToken();
            assertEquals(token, XContentParser.Token.FIELD_NAME);
            assertEquals(field.getPreferredName(), parser.currentName());
            token = parser.nextToken();

            if (field.getPreferredName().equalsIgnoreCase("include")) {
                return IncludeExclude.parseInclude(parser);
            } else if (field.getPreferredName().equalsIgnoreCase("exclude")) {
                return IncludeExclude.parseExclude(parser);
            } else {
                throw new IllegalArgumentException("Unexpected field name serialized in test: " + field.getPreferredName());
            }
        }
    }

    public void testRegexIncludeAndExclude() throws IOException {
        String incRegex = "foo.*";
        String excRegex = "football";
        String differentExcRegex = "foosball";
        IncludeExclude serialized = serializeMixedRegex(new IncludeExclude(incRegex, excRegex));
        assertFalse(serialized.isPartitionBased());
        assertTrue(serialized.isRegexBased());

        IncludeExclude same = new IncludeExclude(incRegex, excRegex);
        assertEquals(serialized, same);
        assertEquals(serialized.hashCode(), same.hashCode());

        IncludeExclude different = new IncludeExclude(incRegex, differentExcRegex);
        assertFalse(serialized.equals(different));
        assertTrue(serialized.hashCode() != different.hashCode());
    }

    // Serializes/deserializes the IncludeExclude statement with include AND
    // exclude clauses
    private IncludeExclude serializeMixedRegex(IncludeExclude incExc) throws IOException {
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        incExc.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(builder)) {
            XContentParser.Token token = parser.nextToken();
            assertEquals(token, XContentParser.Token.START_OBJECT);

            IncludeExclude inc = null;
            IncludeExclude exc = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                assertEquals(XContentParser.Token.FIELD_NAME, token);
                if (IncludeExclude.INCLUDE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    token = parser.nextToken();
                    inc = IncludeExclude.parseInclude(parser);
                } else if (IncludeExclude.EXCLUDE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    token = parser.nextToken();
                    exc = IncludeExclude.parseExclude(parser);
                } else {
                    throw new IllegalArgumentException("Unexpected field name serialized in test: " + parser.currentName());
                }
            }
            assertNotNull(inc);
            assertNotNull(exc);
            // Include and Exclude clauses are parsed independently and then merged
            return IncludeExclude.merge(inc, exc);
        }
    }

    private static BytesRef[] toBytesRefArray(String... values) {
        BytesRef[] bytesRefs = new BytesRef[values.length];
        for (int i = 0; i < values.length; i++) {
            bytesRefs[i] = new BytesRef(values[i]);
        }
        return bytesRefs;
    }

    public void testPrefixOrds() throws IOException {
        IncludeExclude includeExclude = new IncludeExclude("(color|length|size):.*", "color:g.*");

        OrdinalsFilter ordinalsFilter = includeExclude.convertToOrdinalsFilter(DocValueFormat.RAW);

        // Which of the following match the filter or not?
        BytesRef[] bytesRefs = toBytesRefArray(
            "color:blue", // true
            "color:crimson", // true
            "color:green", // false (excluded)
            "color:gray", // false (excluded)
            "depth:10in", // false
            "depth:12in", // false
            "depth:17in", // false
            "length:long", // true
            "length:medium", // true
            "length:short", // true
            "material:cotton", // false
            "material:linen", // false
            "material:polyester", // false
            "size:large", // true
            "size:medium", // true
            "size:small", // true
            "width:13in", // false
            "width:27in", // false
            "width:55in" // false
        );
        boolean[] expectedFilter = new boolean[] {
            true,
            true,
            false,
            false,
            false,
            false,
            false,
            true,
            true,
            true,
            false,
            false,
            false,
            true,
            true,
            true,
            false,
            false,
            false };

        LongAdder lookupCount = new LongAdder();
        SortedSetDocValues sortedSetDocValues = new AbstractSortedSetDocValues() {
            @Override
            public boolean advanceExact(int target) {
                return false;
            }

            @Override
            public long nextOrd() {
                return 0;
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                lookupCount.increment();
                int ordIndex = Math.toIntExact(ord);
                return bytesRefs[ordIndex];
            }

            @Override
            public long getValueCount() {
                return bytesRefs.length;
            }
        };

        LongBitSet acceptedOrds = ordinalsFilter.acceptedGlobalOrdinals(sortedSetDocValues);
        long prefixLookupCount = lookupCount.longValue();
        assertEquals(expectedFilter.length, acceptedOrds.length());
        for (int i = 0; i < expectedFilter.length; i++) {
            assertEquals(expectedFilter[i], acceptedOrds.get(i));
        }

        // Now repeat, but this time, the prefix optimization won't work (because of the .+ on the exclude filter)
        includeExclude = new IncludeExclude("(color|length|size):.*", "color:g.+");
        ordinalsFilter = includeExclude.convertToOrdinalsFilter(DocValueFormat.RAW);
        acceptedOrds = ordinalsFilter.acceptedGlobalOrdinals(sortedSetDocValues);
        long regexpLookupCount = lookupCount.longValue() - prefixLookupCount;

        // The filter should be functionally the same
        assertEquals(expectedFilter.length, acceptedOrds.length());
        for (int i = 0; i < expectedFilter.length; i++) {
            assertEquals(expectedFilter[i], acceptedOrds.get(i));
        }

        // But the full regexp requires more lookups
        assertTrue(regexpLookupCount > prefixLookupCount);
    }

    public void testExtractPrefixes() {
        // Positive tests
        assertEquals(bytesRefs("a"), IncludeExclude.extractPrefixes("a.*", 1000));
        assertEquals(bytesRefs("a", "b"), IncludeExclude.extractPrefixes("(a|b).*", 1000));
        assertEquals(bytesRefs("ab"), IncludeExclude.extractPrefixes("a(b).*", 1000));
        assertEquals(bytesRefs("ab", "ac"), IncludeExclude.extractPrefixes("a(b|c).*", 1000));
        assertEquals(bytesRefs("aabb", "aacc"), IncludeExclude.extractPrefixes("aa(bb|cc).*", 1000));

        // No regex means no prefixes
        assertEquals(bytesRefs(), IncludeExclude.extractPrefixes(null, 1000));

        // Negative tests
        assertNull(IncludeExclude.extractPrefixes(".*", 1000)); // Literal match-all has no prefixes
        assertNull(IncludeExclude.extractPrefixes("ab?.*", 1000)); // Don't support optionals ye
        // The following cover various cases involving infinite possible prefixes
        assertNull(IncludeExclude.extractPrefixes("ab*.*", 1000));
        assertNull(IncludeExclude.extractPrefixes("a*(b|c).*", 1000));
        assertNull(IncludeExclude.extractPrefixes("a(b*|c)d.*", 1000));
        assertNull(IncludeExclude.extractPrefixes("a(b|c*)d.*", 1000));
        assertNull(IncludeExclude.extractPrefixes("a(b|c*)d*.*", 1000));

        // Test with too many possible prefixes -- 10 * 10 * 10 + 1 > 1000
        assertNull(
            IncludeExclude.extractPrefixes(
                "((a1|a2|a3|a4|a5|a6|a7|a8|a9|a10)" + "(b1|b2|b3|b4|b5|b6|b7|b8|b9|b10)" + "(c1|c2|c3|c4|c5|c6|c7|c8|c9|c10)|x).*",
                1000
            )
        );
        // Test with too many possible prefixes -- 10 * 10 * 11 > 1000
        assertNull(
            IncludeExclude.extractPrefixes(
                "((a1|a2|a3|a4|a5|a6|a7|a8|a9|a10)" + "(b1|b2|b3|b4|b5|b6|b7|b8|b9|b10)" + "(c1|c2|c3|c4|c5|c6|c7|c8|c9|c10|c11)).*",
                1000
            )
        );
    }

    private static SortedSet<BytesRef> bytesRefs(String... strings) {
        SortedSet<BytesRef> bytesRefs = new TreeSet<>();
        for (String string : strings) {
            bytesRefs.add(new BytesRef(string));
        }
        return bytesRefs;
    }

    private static SortedSetDocValues toDocValues(BytesRef[] bytesRefs) {
        return new AbstractSortedSetDocValues() {
            @Override
            public boolean advanceExact(int target) {
                return false;
            }

            @Override
            public long nextOrd() {
                return 0;
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                int ordIndex = Math.toIntExact(ord);
                return bytesRefs[ordIndex];
            }

            @Override
            public long getValueCount() {
                return bytesRefs.length;
            }
        };
    }

    public void testOnlyIncludeExcludePrefix() throws IOException {
        String incExcString = "(color:g|width:).*";
        IncludeExclude excludeOnly = new IncludeExclude(null, incExcString);

        OrdinalsFilter ordinalsFilter = excludeOnly.convertToOrdinalsFilter(DocValueFormat.RAW);
        // Which of the following match the filter or not?
        BytesRef[] bytesRefs = toBytesRefArray(
            "color:blue", // true
            "color:crimson", // true
            "color:green", // false (excluded)
            "color:gray", // false (excluded)
            "depth:10in", // true
            "depth:12in", // true
            "depth:17in", // true
            "length:long", // true
            "length:medium", // true
            "length:short", // true
            "material:cotton", // true
            "material:linen", // true
            "material:polyester", // true
            "size:large", // true
            "size:medium", // true
            "size:small", // true
            "width:13in", // false
            "width:27in", // false
            "width:55in" // false
        );
        boolean[] expectedFilter = new boolean[] {
            true,
            true,
            false,
            false,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            false,
            false,
            false };
        LongBitSet longBitSet = ordinalsFilter.acceptedGlobalOrdinals(toDocValues(bytesRefs));

        for (int i = 0; i < expectedFilter.length; i++) {
            assertEquals(expectedFilter[i], longBitSet.get(i));
        }

        // Now repeat, but this time include only.
        IncludeExclude includeOnly = new IncludeExclude(incExcString, null);
        ordinalsFilter = includeOnly.convertToOrdinalsFilter(DocValueFormat.RAW);
        longBitSet = ordinalsFilter.acceptedGlobalOrdinals(toDocValues(bytesRefs));

        // The previously excluded ordinals should be included and vice versa.
        for (int i = 0; i < expectedFilter.length; i++) {
            assertEquals(!expectedFilter[i], longBitSet.get(i));
        }
    }
}
