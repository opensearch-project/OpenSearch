/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ExtraFieldValueTests extends OpenSearchTestCase {

    public void testBytesValueWriteReadRoundTrip() throws Exception {
        BytesValue v = new BytesValue(new BytesArray(new byte[] { 1, 2, 3, 4 }));

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(BytesValue.class));
        BytesValue bv = (BytesValue) read;
        assertThat(bv.type(), is(ExtraFieldValue.Type.BYTES));
        assertThat(bv.size(), is(4));
        BytesRef bytesRef = bv.bytes().toBytesRef();
        assertThat(bytesRef.length, is(4));
        assertThat(bytesRef.bytes[bytesRef.offset], is((byte) 1));
    }

    public void testFloatArrayWriteReadRoundTrip() throws Exception {
        FloatArrayValue v = FloatArrayValue.fromFloatArray(new float[] { 10.5f, -2.25f });

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(FloatArrayValue.class));
        FloatArrayValue fav = (FloatArrayValue) read;

        assertThat(fav.type(), is(ExtraFieldValue.Type.FLOAT_ARRAY));
        assertThat(fav.dimension(), is(2));
        assertEquals(10.5f, fav.get(0), 0.0f);
        assertEquals(-2.25f, fav.get(1), 0.0f);
    }

    public void testIntArrayWriteReadRoundTrip() throws Exception {
        IntArrayValue v = IntArrayValue.fromIntArray(new int[] { 10, -2 });

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(IntArrayValue.class));
        IntArrayValue iav = (IntArrayValue) read;

        assertThat(iav.type(), is(ExtraFieldValue.Type.INT_ARRAY));
        assertThat(iav.dimension(), is(2));
        assertThat(iav.get(0), is(10));
        assertThat(iav.get(1), is(-2));
    }

    public void testLongArrayWriteReadRoundTrip() throws Exception {
        LongArrayValue v = LongArrayValue.fromLongArray(new long[] { 10L, -2L });

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(LongArrayValue.class));
        LongArrayValue lav = (LongArrayValue) read;

        assertThat(lav.type(), is(ExtraFieldValue.Type.LONG_ARRAY));
        assertThat(lav.dimension(), is(2));
        assertThat(lav.get(0), is(10L));
        assertThat(lav.get(1), is(-2L));
    }

    public void testDoubleArrayWriteReadRoundTrip() throws Exception {
        DoubleArrayValue v = DoubleArrayValue.fromDoubleArray(new double[] { 10.5d, -2.25d });

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(DoubleArrayValue.class));
        DoubleArrayValue dav = (DoubleArrayValue) read;

        assertThat(dav.type(), is(ExtraFieldValue.Type.DOUBLE_ARRAY));
        assertThat(dav.dimension(), is(2));
        assertEquals(10.5d, dav.get(0), 0.0d);
        assertEquals(-2.25d, dav.get(1), 0.0d);
    }

    public void testPackedIntArrayWriteReadRoundTrip() throws Exception {
        int[] vals = new int[] { 10, -2 };
        IntArrayValue v = IntArrayValue.fromPackedArray(packIntLE(vals), vals.length);

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(IntArrayValue.class));
        IntArrayValue iav = (IntArrayValue) read;

        assertThat(iav.type(), is(ExtraFieldValue.Type.INT_ARRAY));
        assertThat(iav.isPackedLE(), is(true));
        assertArrayEquals(vals, iav.asIntArray());
    }

    public void testPackedLongArrayWriteReadRoundTrip() throws Exception {
        long[] vals = new long[] { 10L, -2L };
        LongArrayValue v = LongArrayValue.fromPackedArray(packLongLE(vals), vals.length);

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(LongArrayValue.class));
        LongArrayValue lav = (LongArrayValue) read;

        assertThat(lav.type(), is(ExtraFieldValue.Type.LONG_ARRAY));
        assertThat(lav.isPackedLE(), is(true));
        assertArrayEquals(vals, lav.asLongArray());
    }

    public void testPackedDoubleArrayWriteReadRoundTrip() throws Exception {
        double[] vals = new double[] { 10.5d, -2.25d };
        DoubleArrayValue v = DoubleArrayValue.fromPackedArray(packDoubleLE(vals), vals.length);

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(DoubleArrayValue.class));
        DoubleArrayValue dav = (DoubleArrayValue) read;

        assertThat(dav.type(), is(ExtraFieldValue.Type.DOUBLE_ARRAY));
        assertThat(dav.isPackedLE(), is(true));
        assertArrayEquals(vals, dav.asDoubleArray(), 0.0d);
    }

    public void testUnknownTypeIdThrows() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // write unknown type id, then some junk body
        out.writeByte((byte) 42);
        out.writeVInt(0);

        StreamInput in = out.bytes().streamInput();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ExtraFieldValue.readFrom(in));
        assertThat(e.getMessage(), containsString("Unknown ExtraFieldValue.Type id"));
    }

    private static byte[] packIntLE(int[] vals) {
        ByteBuffer buffer = ByteBuffer.allocate(vals.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value : vals) {
            buffer.putInt(value);
        }
        return buffer.array();
    }

    private static byte[] packLongLE(long[] vals) {
        ByteBuffer buffer = ByteBuffer.allocate(vals.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long value : vals) {
            buffer.putLong(value);
        }
        return buffer.array();
    }

    private static byte[] packDoubleLE(double[] vals) {
        ByteBuffer buffer = ByteBuffer.allocate(vals.length * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (double value : vals) {
            buffer.putDouble(value);
        }
        return buffer.array();
    }
}
