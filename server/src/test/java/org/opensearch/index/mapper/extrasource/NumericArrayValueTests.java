/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class NumericArrayValueTests extends OpenSearchTestCase {

    public void testPrimitiveIntArray() throws Exception {
        int[] vals = new int[] { 10, -2, 0, Integer.MIN_VALUE, Integer.MAX_VALUE };
        IntArrayValue array = IntArrayValue.fromIntArray(vals);

        assertThat(array.dimension(), is(vals.length));
        assertThat(array.isPackedLE(), is(false));
        assertThat(array.asIntArray(), sameInstance(vals));
        for (int i = 0; i < vals.length; i++) {
            assertThat(array.get(i), is(vals[i]));
        }
        expectThrows(IllegalStateException.class, array::packedBytes);

        BytesStreamOutput out = new BytesStreamOutput();
        array.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        PrimitiveIntArray read = PrimitiveIntArray.readBodyFrom(in);
        assertArrayEquals(vals, read.asIntArray());
    }

    public void testPackedIntArray() throws Exception {
        int[] vals = new int[] { 10, -2, 0, Integer.MIN_VALUE, Integer.MAX_VALUE };
        IntArrayValue array = IntArrayValue.fromPackedArray(packIntLE(vals), vals.length);

        assertThat(array.dimension(), is(vals.length));
        assertThat(array.isPackedLE(), is(true));
        for (int i = 0; i < vals.length; i++) {
            assertThat(array.get(i), is(vals[i]));
        }
        assertArrayEquals(vals, array.asIntArray());
        assertThat(array.asIntArray(), sameInstance(array.asIntArray()));

        BytesStreamOutput out = new BytesStreamOutput();
        array.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        assertThat(in.readVInt(), is(vals.length));
        PackedIntArray read = PackedIntArray.readBodyFrom(in, vals.length);
        assertArrayEquals(vals, read.asIntArray());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> IntArrayValue.fromPackedArray(new byte[3], 1));
        assertThat(e.getMessage(), containsString("Bad packed int length"));
        expectThrows(IndexOutOfBoundsException.class, () -> array.get(-1));
        expectThrows(IndexOutOfBoundsException.class, () -> array.get(vals.length));
    }

    public void testPrimitiveLongArray() throws Exception {
        long[] vals = new long[] { 10L, -2L, 0L, Long.MIN_VALUE, Long.MAX_VALUE };
        LongArrayValue array = LongArrayValue.fromLongArray(vals);

        assertThat(array.dimension(), is(vals.length));
        assertThat(array.isPackedLE(), is(false));
        assertThat(array.asLongArray(), sameInstance(vals));
        for (int i = 0; i < vals.length; i++) {
            assertThat(array.get(i), is(vals[i]));
        }
        expectThrows(IllegalStateException.class, array::packedBytes);

        BytesStreamOutput out = new BytesStreamOutput();
        array.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        PrimitiveLongArray read = PrimitiveLongArray.readBodyFrom(in);
        assertArrayEquals(vals, read.asLongArray());
    }

    public void testPackedLongArray() throws Exception {
        long[] vals = new long[] { 10L, -2L, 0L, Long.MIN_VALUE, Long.MAX_VALUE };
        LongArrayValue array = LongArrayValue.fromPackedArray(packLongLE(vals), vals.length);

        assertThat(array.dimension(), is(vals.length));
        assertThat(array.isPackedLE(), is(true));
        for (int i = 0; i < vals.length; i++) {
            assertThat(array.get(i), is(vals[i]));
        }
        assertArrayEquals(vals, array.asLongArray());
        assertThat(array.asLongArray(), sameInstance(array.asLongArray()));

        BytesStreamOutput out = new BytesStreamOutput();
        array.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        assertThat(in.readVInt(), is(vals.length));
        PackedLongArray read = PackedLongArray.readBodyFrom(in, vals.length);
        assertArrayEquals(vals, read.asLongArray());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LongArrayValue.fromPackedArray(new byte[7], 1));
        assertThat(e.getMessage(), containsString("Bad packed long length"));
        expectThrows(IndexOutOfBoundsException.class, () -> array.get(-1));
        expectThrows(IndexOutOfBoundsException.class, () -> array.get(vals.length));
    }

    public void testPrimitiveDoubleArray() throws Exception {
        double[] vals = new double[] { 10.5d, -2.25d, 0d, Double.MIN_VALUE, Double.MAX_VALUE };
        DoubleArrayValue array = DoubleArrayValue.fromDoubleArray(vals);

        assertThat(array.dimension(), is(vals.length));
        assertThat(array.isPackedLE(), is(false));
        assertThat(array.asDoubleArray(), sameInstance(vals));
        for (int i = 0; i < vals.length; i++) {
            assertEquals(vals[i], array.get(i), 0.0d);
        }
        expectThrows(IllegalStateException.class, array::packedBytes);

        BytesStreamOutput out = new BytesStreamOutput();
        array.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        PrimitiveDoubleArray read = PrimitiveDoubleArray.readBodyFrom(in);
        assertArrayEquals(vals, read.asDoubleArray(), 0.0d);
    }

    public void testPackedDoubleArray() throws Exception {
        double[] vals = new double[] { 10.5d, -2.25d, 0d, Double.MIN_VALUE, Double.MAX_VALUE };
        DoubleArrayValue array = DoubleArrayValue.fromPackedArray(packDoubleLE(vals), vals.length);

        assertThat(array.dimension(), is(vals.length));
        assertThat(array.isPackedLE(), is(true));
        for (int i = 0; i < vals.length; i++) {
            assertEquals(vals[i], array.get(i), 0.0d);
        }
        assertArrayEquals(vals, array.asDoubleArray(), 0.0d);
        assertThat(array.asDoubleArray(), sameInstance(array.asDoubleArray()));

        BytesStreamOutput out = new BytesStreamOutput();
        array.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        assertThat(in.readVInt(), is(vals.length));
        PackedDoubleArray read = PackedDoubleArray.readBodyFrom(in, vals.length);
        assertArrayEquals(vals, read.asDoubleArray(), 0.0d);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DoubleArrayValue.fromPackedArray(new byte[7], 1));
        assertThat(e.getMessage(), containsString("Bad packed double length"));
        expectThrows(IndexOutOfBoundsException.class, () -> array.get(-1));
        expectThrows(IndexOutOfBoundsException.class, () -> array.get(vals.length));
    }

    public void testPrimitiveArrayFactoriesRejectNull() {
        NullPointerException floatError = expectThrows(NullPointerException.class, () -> FloatArrayValue.fromFloatArray(null));
        assertThat(floatError.getMessage(), containsString("values must not be null"));

        NullPointerException intError = expectThrows(NullPointerException.class, () -> IntArrayValue.fromIntArray(null));
        assertThat(intError.getMessage(), containsString("values must not be null"));

        NullPointerException longError = expectThrows(NullPointerException.class, () -> LongArrayValue.fromLongArray(null));
        assertThat(longError.getMessage(), containsString("values must not be null"));

        NullPointerException doubleError = expectThrows(NullPointerException.class, () -> DoubleArrayValue.fromDoubleArray(null));
        assertThat(doubleError.getMessage(), containsString("values must not be null"));
    }

    public void testPackedArrayFactoriesRejectNull() {
        NullPointerException floatError = expectThrows(NullPointerException.class, () -> FloatArrayValue.fromPackedArray(null, 1));
        assertThat(floatError.getMessage(), containsString("packedArray must not be null"));

        NullPointerException intError = expectThrows(NullPointerException.class, () -> IntArrayValue.fromPackedArray(null, 1));
        assertThat(intError.getMessage(), containsString("packedArray must not be null"));

        NullPointerException longError = expectThrows(NullPointerException.class, () -> LongArrayValue.fromPackedArray(null, 1));
        assertThat(longError.getMessage(), containsString("packedArray must not be null"));

        NullPointerException doubleError = expectThrows(NullPointerException.class, () -> DoubleArrayValue.fromPackedArray(null, 1));
        assertThat(doubleError.getMessage(), containsString("packedArray must not be null"));
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
