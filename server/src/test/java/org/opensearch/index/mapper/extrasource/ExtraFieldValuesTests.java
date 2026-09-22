/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExtraFieldValuesTests extends OpenSearchTestCase {

    public void testEmptySingleton() {
        assertThat(ExtraFieldValues.EMPTY.isEmpty(), is(true));
        assertThat(ExtraFieldValues.EMPTY.values().isEmpty(), is(true));
        assertThat(ExtraFieldValues.EMPTY.get("does_not_exist"), nullValue());
    }

    public void testStreamConstructorEmptyMap() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        ExtraFieldValues.EMPTY.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValues read = new ExtraFieldValues(in);

        assertThat(read.isEmpty(), is(true));
        assertThat(read.values().isEmpty(), is(true));
        expectThrows(UnsupportedOperationException.class, () -> read.values().put("x", new BytesValue(new BytesArray(new byte[] { 1 }))));
    }

    public void testConstructorCopiesAndIsImmutable() {
        Map<String, ExtraFieldValue> m = new HashMap<>();
        m.put("a", new BytesValue(new BytesArray(new byte[] { 1 })));

        ExtraFieldValues efv = new ExtraFieldValues(m);

        // Mutating original map must not affect efv (Map.copyOf)
        m.put("b", new BytesValue(new BytesArray(new byte[] { 2 })));
        assertThat(efv.get("b"), nullValue());

        // Returned values map should be unmodifiable
        expectThrows(UnsupportedOperationException.class, () -> efv.values().put("x", efv.get("a")));
    }

    public void testWriteReadRoundTrip() throws Exception {
        ExtraFieldValues efv = new ExtraFieldValues(
            Map.of(
                "field_bytes",
                new BytesValue(new BytesArray(new byte[] { 9, 8, 7 })),
                "field_vec",
                FloatArrayValue.fromFloatArray(new float[] { 1.25f, -3.75f }),
                "field_ints",
                IntArrayValue.fromIntArray(new int[] { 10, -20 }),
                "field_longs",
                LongArrayValue.fromLongArray(new long[] { 100L, -200L }),
                "field_doubles",
                DoubleArrayValue.fromDoubleArray(new double[] { 2.5d, -4.5d })
            )
        );

        BytesStreamOutput out = new BytesStreamOutput();
        efv.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValues read = new ExtraFieldValues(in);

        assertThat(read.isEmpty(), is(false));
        assertThat(read.values().keySet(), containsInAnyOrder("field_bytes", "field_vec", "field_ints", "field_longs", "field_doubles"));

        ExtraFieldValue v1 = read.get("field_bytes");
        assertThat(v1, instanceOf(BytesValue.class));
        assertThat(v1.type(), is(ExtraFieldValue.Type.BYTES));
        assertThat(v1.size(), is(3));

        ExtraFieldValue v2 = read.get("field_vec");
        assertThat(v2, instanceOf(FloatArrayValue.class));
        FloatArrayValue fav = (FloatArrayValue) v2;
        assertThat(fav.type(), is(ExtraFieldValue.Type.FLOAT_ARRAY));
        assertThat(fav.dimension(), is(2));
        assertEquals(1.25f, fav.get(0), 0.0f);
        assertEquals(-3.75f, fav.get(1), 0.0f);

        ExtraFieldValue v3 = read.get("field_ints");
        assertThat(v3, instanceOf(IntArrayValue.class));
        IntArrayValue iav = (IntArrayValue) v3;
        assertThat(iav.type(), is(ExtraFieldValue.Type.INT_ARRAY));
        assertThat(iav.dimension(), is(2));
        assertThat(iav.get(0), is(10));
        assertThat(iav.get(1), is(-20));

        ExtraFieldValue v4 = read.get("field_longs");
        assertThat(v4, instanceOf(LongArrayValue.class));
        LongArrayValue lav = (LongArrayValue) v4;
        assertThat(lav.type(), is(ExtraFieldValue.Type.LONG_ARRAY));
        assertThat(lav.dimension(), is(2));
        assertThat(lav.get(0), is(100L));
        assertThat(lav.get(1), is(-200L));

        ExtraFieldValue v5 = read.get("field_doubles");
        assertThat(v5, instanceOf(DoubleArrayValue.class));
        DoubleArrayValue dav = (DoubleArrayValue) v5;
        assertThat(dav.type(), is(ExtraFieldValue.Type.DOUBLE_ARRAY));
        assertThat(dav.dimension(), is(2));
        assertEquals(2.5d, dav.get(0), 0.0d);
        assertEquals(-4.5d, dav.get(1), 0.0d);
    }
}
