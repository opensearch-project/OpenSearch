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

package org.opensearch.search;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.mapper.DateFieldMapper.Resolution;
import org.opensearch.test.OpenSearchTestCase;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.search.aggregations.bucket.GeoTileUtils.longEncode;

public class DocValueFormatTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.GEOTILE.getWriteableName(), in -> DocValueFormat.GEOTILE));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.BINARY.getWriteableName(), in -> DocValueFormat.BINARY));
        NamedWriteableRegistry registry = new NamedWriteableRegistry(entries);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.BOOLEAN);
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.BOOLEAN, in.readNamedWriteable(DocValueFormat.class));

        DocValueFormat.Decimal decimalFormat = new DocValueFormat.Decimal("###.##");
        out = new BytesStreamOutput();
        out.writeNamedWriteable(decimalFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        DocValueFormat vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.Decimal.class, vf.getClass());
        assertEquals("###.##", ((DocValueFormat.Decimal) vf).pattern);
        assertEquals(decimalFormat, vf);

        DateFormatter formatter = DateFormatter.forPattern("epoch_second");
        DocValueFormat.DateTime dateFormat = new DocValueFormat.DateTime(formatter, ZoneOffset.ofHours(1), Resolution.MILLISECONDS);
        out = new BytesStreamOutput();
        out.writeNamedWriteable(dateFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.pattern());
        assertEquals(ZoneOffset.ofHours(1), ((DocValueFormat.DateTime) vf).timeZone);
        assertEquals(Resolution.MILLISECONDS, ((DocValueFormat.DateTime) vf).resolution);
        assertEquals(dateFormat, vf);

        DocValueFormat.DateTime nanosDateFormat = new DocValueFormat.DateTime(formatter, ZoneOffset.ofHours(1), Resolution.NANOSECONDS);
        out = new BytesStreamOutput();
        out.writeNamedWriteable(nanosDateFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.pattern());
        assertEquals(ZoneOffset.ofHours(1), ((DocValueFormat.DateTime) vf).timeZone);
        assertEquals(Resolution.NANOSECONDS, ((DocValueFormat.DateTime) vf).resolution);
        assertEquals(nanosDateFormat, vf);

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.GEOHASH);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.GEOHASH, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.GEOTILE);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.GEOTILE, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.IP);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.IP, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.RAW);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.RAW, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.BINARY);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.BINARY, in.readNamedWriteable(DocValueFormat.class));
    }

    public void testRawFormat() {
        assertEquals(0L, DocValueFormat.RAW.format(0));
        assertEquals(-1L, DocValueFormat.RAW.format(-1));
        assertEquals(1L, DocValueFormat.RAW.format(1));

        assertEquals(0d, DocValueFormat.RAW.format(0d));
        assertEquals(.5d, DocValueFormat.RAW.format(.5d));
        assertEquals(-1d, DocValueFormat.RAW.format(-1d));

        assertEquals("abc", DocValueFormat.RAW.format(new BytesRef("abc")));
    }

    public void testBinaryFormat() {
        assertEquals("", DocValueFormat.BINARY.format(new BytesRef()));
        assertEquals("KmQ", DocValueFormat.BINARY.format(new BytesRef(new byte[] { 42, 100 })));

        assertEquals(new BytesRef(), DocValueFormat.BINARY.parseBytesRef(""));
        assertEquals(new BytesRef(new byte[] { 42, 100 }), DocValueFormat.BINARY.parseBytesRef("KmQ"));
    }

    public void testBooleanFormat() {
        assertEquals(false, DocValueFormat.BOOLEAN.format(0));
        assertEquals(true, DocValueFormat.BOOLEAN.format(1));
    }

    public void testIpFormat() {
        assertEquals(
            "192.168.1.7",
            DocValueFormat.IP.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.7"))))
        );
        assertEquals("::1", DocValueFormat.IP.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1")))));
    }

    public void testDecimalFormat() {
        DocValueFormat formatter = new DocValueFormat.Decimal("###.##");
        assertEquals("0", formatter.format(0.0d));
        assertEquals("1", formatter.format(1d));
        formatter = new DocValueFormat.Decimal("000.000");
        assertEquals("-000.500", formatter.format(-0.5));
        formatter = new DocValueFormat.Decimal("###,###.###");
        assertEquals("0.86", formatter.format(0.8598023539251286d));
        formatter = new DocValueFormat.Decimal("###,###.###");
        assertEquals("859,802.354", formatter.format(0.8598023539251286d * 1_000_000));
    }

    public void testGeoTileFormat() {
        assertEquals("0/0/0", DocValueFormat.GEOTILE.format(longEncode(0, 0, 0)));
        assertEquals("15/19114/7333", DocValueFormat.GEOTILE.format(longEncode(30, 70, 15)));
        assertEquals("29/536869420/0", DocValueFormat.GEOTILE.format(longEncode(179.999, 89.999, 29)));
        assertEquals("29/1491/536870911", DocValueFormat.GEOTILE.format(longEncode(-179.999, -89.999, 29)));
        assertEquals("2/2/1", DocValueFormat.GEOTILE.format(longEncode(1, 1, 2)));
        assertEquals("1/1/0", DocValueFormat.GEOTILE.format(longEncode(13, 95, 1)));
        assertEquals("1/1/1", DocValueFormat.GEOTILE.format(longEncode(13, -95, 1)));
    }

    public void testRawParse() {
        assertEquals(-1L, DocValueFormat.RAW.parseLong("-1", randomBoolean(), null));
        assertEquals(1L, DocValueFormat.RAW.parseLong("1", randomBoolean(), null));
        assertEquals(Long.MAX_VALUE - 2, DocValueFormat.RAW.parseLong(Long.toString(Long.MAX_VALUE - 2), randomBoolean(), null));
        // not checking exception messages as they could depend on the JVM
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("", randomBoolean(), null));
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("abc", randomBoolean(), null));

        assertEquals(-1d, DocValueFormat.RAW.parseDouble("-1", randomBoolean(), null), 0d);
        assertEquals(1d, DocValueFormat.RAW.parseDouble("1", randomBoolean(), null), 0d);
        assertEquals(.5, DocValueFormat.RAW.parseDouble("0.5", randomBoolean(), null), 0d);
        // not checking exception messages as they could depend on the JVM
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseDouble("", randomBoolean(), null));
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseDouble("abc", randomBoolean(), null));

        assertEquals(new BytesRef("abc"), DocValueFormat.RAW.parseBytesRef("abc"));
    }

    public void testBooleanParse() {
        assertEquals(0L, DocValueFormat.BOOLEAN.parseLong("false", randomBoolean(), null));
        assertEquals(1L, DocValueFormat.BOOLEAN.parseLong("true", randomBoolean(), null));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DocValueFormat.BOOLEAN.parseLong("", randomBoolean(), null)
        );
        assertEquals("Cannot parse boolean [], expected either [true] or [false]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> DocValueFormat.BOOLEAN.parseLong("0", randomBoolean(), null));
        assertEquals("Cannot parse boolean [0], expected either [true] or [false]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> DocValueFormat.BOOLEAN.parseLong("False", randomBoolean(), null));
        assertEquals("Cannot parse boolean [False], expected either [true] or [false]", e.getMessage());
    }

    public void testIPParse() {
        assertEquals(
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.7"))),
            DocValueFormat.IP.parseBytesRef("192.168.1.7")
        );
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), DocValueFormat.IP.parseBytesRef("::1"));
    }

    public void testDecimalParse() {
        DocValueFormat parser = new DocValueFormat.Decimal("###.##");
        assertEquals(0.0d, parser.parseDouble(randomFrom("0.0", "0", ".0", ".0000"), true, null), 0.0d);
        assertEquals(-1.0d, parser.parseDouble(randomFrom("-1.0", "-1", "-1.0", "-1.0000"), true, null), 0.0d);
        assertEquals(0.0d, parser.parseLong("0", true, null), 0.0d);
        assertEquals(1.0d, parser.parseLong("1", true, null), 0.0d);
        parser = new DocValueFormat.Decimal("###,###.###");
        assertEquals(859802.354d, parser.parseDouble("859,802.354", true, null), 0.0d);
        assertEquals(0.859d, parser.parseDouble("0.859", true, null), 0.0d);
        assertEquals(0.8598023539251286d, parser.parseDouble("0.8598023539251286", true, null), 0.0d);
    }

    public void testLongParse() {
        assertEquals(DocValueFormat.RAW.format(0), 0L);
        assertEquals(DocValueFormat.RAW.format(-1), -1L);
        assertEquals(DocValueFormat.RAW.format(1), 1L);
        assertEquals(DocValueFormat.RAW.format(0d), 0d);
        assertEquals(DocValueFormat.RAW.format(9.5d), 9.5d);
        assertEquals(DocValueFormat.RAW.format(-1d), -1d);
    }

    public void testGeoTileParse() {
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(0, 0, 0)), "0/0/0");
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(30, 70, 15)), "15/19114/7333");
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(179.999, 89.999, 29)), "29/536869420/0");
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(-179.999, -89.999, 29)), "29/1491/536870911");
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(1, 1, 2)), "2/2/1");
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(13, 95, 1)), "1/1/0");
        assertEquals(DocValueFormat.GEOTILE.format(longEncode(13, -95, 1)), "1/1/1");
    }
}
