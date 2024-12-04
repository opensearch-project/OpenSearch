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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IpFieldTypeTests extends FieldTypeTestCase {

    public void testValueFormat() throws Exception {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForDisplay(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForDisplay(asBytes));
    }

    public void testTermQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field", true, false, true, null, Collections.emptyMap());

        String ip = "2001:db8::2:1";

        Query query = InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip));

        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowExactQuery("field", new BytesRef(((PointRangeQuery) query).getLowerPoint()))
            ),
            ft.termQuery(ip, null)
        );

        ip = "192.168.1.7";
        query = InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowExactQuery("field", new BytesRef(((PointRangeQuery) query).getLowerPoint()))
            ),
            ft.termQuery(ip, null)
        );

        ip = "2001:db8::2:1";
        String prefix = ip + "/64";

        query = InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 64);
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    "field",
                    ipToByteRef("2001:db8:0:0:0:0:0:0"),
                    ipToByteRef("2001:db8:0:0:ffff:ffff:ffff:ffff"),
                    true,
                    true
                )
            ),
            ft.termQuery(prefix, null)
        );

        ip = "192.168.1.7";
        prefix = ip + "/16";
        query = InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 16);
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    "field",
                    ipToByteRef("::ffff:192.168.0.0"),
                    ipToByteRef("::ffff:192.168.255.255"),
                    true,
                    true
                )
            ),
            ft.termQuery(prefix, null)
        );

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType("field", false, false, false, null, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("::1", null));
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testDvOnlyTermQuery() {
        IpFieldMapper.IpFieldType dvOnly = new IpFieldMapper.IpFieldType("field", false, false, true, null, Collections.emptyMap());
        String ip = "2001:db8::2:1";

        Query query = InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip));

        assertEquals(
            SortedSetDocValuesField.newSlowExactQuery("field", new BytesRef(((PointRangeQuery) query).getLowerPoint())),
            dvOnly.termQuery(ip, null)
        );

        ip = "192.168.1.7";
        query = InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip));
        assertEquals(
            SortedSetDocValuesField.newSlowExactQuery("field", new BytesRef(((PointRangeQuery) query).getLowerPoint())),
            dvOnly.termQuery(ip, null)
        );

        ip = "2001:db8::2:1";
        String prefix = ip + "/64";

        assertEquals(
            SortedSetDocValuesField.newSlowRangeQuery(
                "field",
                ipToByteRef("2001:db8:0:0:0:0:0:0"),
                ipToByteRef("2001:db8:0:0:ffff:ffff:ffff:ffff"),
                true,
                true
            ),
            dvOnly.termQuery(prefix, null)
        );

        ip = "192.168.1.7";
        prefix = ip + "/16";
        assertEquals(
            SortedSetDocValuesField.newSlowRangeQuery(
                "field",
                ipToByteRef("::ffff:192.168.0.0"),
                ipToByteRef("::ffff:192.168.255.255"),
                true,
                true
            ),
            dvOnly.termQuery(prefix, null)
        );
    }

    private static BytesRef ipToByteRef(String ipString) {
        return new BytesRef(Objects.requireNonNull(InetAddresses.ipStringToBytes(ipString)));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field", true, false, false, null, Collections.emptyMap());

        assertEquals(
            InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
            ft.termsQuery(Arrays.asList(InetAddresses.forString("::2"), InetAddresses.forString("::5")), null)
        );
        assertEquals(
            InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
            ft.termsQuery(Arrays.asList("::2", "::5"), null)
        );

        // if the list includes a prefix query we fallback to a bool query
        Query actual = ft.termsQuery(Arrays.asList("::42", "::2/16"), null);
        assertTrue(actual instanceof ConstantScoreQuery);
        assertTrue(((ConstantScoreQuery) actual).getQuery() instanceof BooleanQuery);
        BooleanQuery bq = (BooleanQuery) ((ConstantScoreQuery) actual).getQuery();
        assertEquals(2, bq.clauses().size());
        assertTrue(bq.clauses().stream().allMatch(c -> c.getOccur() == Occur.SHOULD));
    }

    public void testDvOnlyTermsQuery() {
        MappedFieldType dvOnly = new IpFieldMapper.IpFieldType("field", false, false, true, null, Collections.emptyMap());

        assertEquals(
            SortedSetDocValuesField.newSlowSetQuery("field", List.of(ipToByteRef("::2"), ipToByteRef("::5"))),
            dvOnly.termsQuery(Arrays.asList(InetAddresses.forString("::2"), InetAddresses.forString("::5")), null)
        );
        assertEquals(
            SortedSetDocValuesField.newSlowSetQuery("field", List.of(ipToByteRef("::2"), ipToByteRef("::5"))),
            dvOnly.termsQuery(Arrays.asList("::2", "::5"), null)
        );

        // if the list includes a prefix query we fallback to a bool query
        assertEquals(
            new ConstantScoreQuery(
                new BooleanQuery.Builder().add(dvOnly.termQuery("::42", null), Occur.SHOULD)
                    .add(dvOnly.termQuery("::2/16", null), Occur.SHOULD)
                    .build()
            ),
            dvOnly.termsQuery(Arrays.asList("::42", "::2/16"), null)
        );
    }

    public void testDvVsPoint() {
        MappedFieldType indexOnly = new IpFieldMapper.IpFieldType("field", true, false, false, null, Collections.emptyMap());
        MappedFieldType dvOnly = new IpFieldMapper.IpFieldType("field", false, false, true, null, Collections.emptyMap());
        MappedFieldType indexDv = new IpFieldMapper.IpFieldType("field", true, false, true, null, Collections.emptyMap());
        assertEquals("ignore DV", indexOnly.termsQuery(List.of("::2/16"), null), indexDv.termsQuery(List.of("::2/16"), null));
        assertEquals(dvOnly.termQuery("::2/16", null), dvOnly.termsQuery(List.of("::2/16"), null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        Query query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("192.168.2.0"));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery(null, "192.168.2.0", randomBoolean(), true, null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("192.168.1.255"));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery(null, "192.168.2.0", randomBoolean(), false, null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery("2001:db8::", null, true, randomBoolean(), null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::1"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery("2001:db8::", null, false, randomBoolean(), null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::"), InetAddresses.forString("2001:db8::ffff"));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery("2001:db8::", "2001:db8::ffff", true, true, null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::1"), InetAddresses.forString("2001:db8::fffe"));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery("2001:db8::", "2001:db8::ffff", false, false, null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("2001:db8::2"), InetAddresses.forString("2001:db8::"));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("2001:db8::1", "2001:db8::1", false, false, null, null, null, null)
        );

        // Upper bound is the min IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("::", "::", true, false, null, null, null, null));

        // Lower bound is the max IP and is not inclusive
        assertEquals(
            new MatchNoDocsQuery(),
            ft.rangeQuery(
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                false,
                true,
                null,
                null,
                null,
                null
            )
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::"), InetAddresses.forString("::fffe:ffff:ffff"));
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("::", "0.0.0.0", true, false, null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("::1:0:0:0"), InetAddressPoint.MAX_VALUE);
        assertEquals(
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            // same lo/hi values but inclusive=false so this won't match anything
            ft.rangeQuery("255.255.255.255", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false, true, null, null, null, null)
        );

        query = InetAddressPoint.newRangeQuery("field", InetAddresses.forString("192.168.1.7"), InetAddresses.forString("2001:db8::"));
        assertEquals(
            // lower bound is ipv4, upper bound is ipv6
            new IndexOrDocValuesQuery(
                query,
                SortedSetDocValuesField.newSlowRangeQuery(
                    ((PointRangeQuery) query).getField(),
                    new BytesRef(((PointRangeQuery) query).getLowerPoint()),
                    new BytesRef(((PointRangeQuery) query).getUpperPoint()),
                    true,
                    true
                )
            ),
            ft.rangeQuery("::ffff:c0a8:107", "2001:db8::", true, true, null, null, null, null)
        );

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType("field", false, false, false, null, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery("::1", "2001::", true, true, null, null, null, null)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new IpFieldMapper.Builder("field", true, Version.CURRENT).build(context).fieldType();
        assertEquals(Collections.singletonList("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8::2:1"));
        assertEquals(Collections.singletonList("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8:0:0:0:0:2:1"));
        assertEquals(Collections.singletonList("::1"), fetchSourceValue(mapper, "0:0:0:0:0:0:0:1"));

        MappedFieldType nullValueMapper = new IpFieldMapper.Builder("field", true, Version.CURRENT).nullValue("2001:db8:0:0:0:0:2:7")
            .build(context)
            .fieldType();
        assertEquals(Collections.singletonList("2001:db8::2:7"), fetchSourceValue(nullValueMapper, null));
    }
}
