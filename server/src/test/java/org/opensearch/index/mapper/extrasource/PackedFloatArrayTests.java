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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PackedFloatArrayTests extends OpenSearchTestCase {

    public void testFromPackedArray_getAndAsFloatArray() {
        final float[] vals = new float[] { 10.5f, -2.25f, 0f, 123.75f };
        final byte[] packed = packLE(vals);

        PackedFloatArray pfa = PackedFloatArray.fromPackedArray(packed, vals.length);

        assertThat(pfa.dimension(), is(vals.length));
        assertThat(pfa.isPackedLE(), is(true));

        for (int i = 0; i < vals.length; i++) {
            assertEquals("mismatch at i=" + i, vals[i], pfa.get(i), 0.0f);
        }

        assertThat(pfa.asFloatArray(), equalTo(vals));

        // cached path should keep returning correct values
        assertEquals(vals[2], pfa.get(2), 0.0f);
    }

    public void testFromPackedBytes_bytesArraySliceUsesUnderlyingArrayWithoutCompaction() {
        final float[] vals = new float[] { 1.25f, 2.5f, -3.75f };
        final byte[] packed = packLE(vals);

        final byte[] backing = new byte[packed.length + 17];
        final int off = 7;
        System.arraycopy(packed, 0, backing, off, packed.length);

        BytesReference ref = new BytesArray(backing, off, packed.length);
        PackedFloatArray pfa = PackedFloatArray.fromPackedBytes(ref, vals.length);

        assertEquals(vals[0], pfa.get(0), 0.0f);

        float mutated = 9.5f;
        writeFloatLE(backing, off, mutated);

        assertEquals(mutated, pfa.get(0), 0.0f);
        assertEquals(vals[1], pfa.get(1), 0.0f);
        assertEquals(vals[2], pfa.get(2), 0.0f);
    }

    public void testFromPackedBytes_compositeMaterializesOnceAndDecodesCorrectly() {
        final float[] vals = new float[] { 0.5f, 1.5f, 2.5f, 3.5f, -4.5f };
        final byte[] packed = packLE(vals);

        // Split into two segments and wrap as CompositeBytesReference
        int mid = packed.length / 2;
        byte[] segA = new byte[mid];
        byte[] segB = new byte[packed.length - mid];
        System.arraycopy(packed, 0, segA, 0, mid);
        System.arraycopy(packed, mid, segB, 0, segB.length);

        BytesReference a = new BytesArray(segA);
        BytesReference b = new BytesArray(segB);
        BytesReference composite = CompositeBytesReference.of(a, b);

        PackedFloatArray pfa = PackedFloatArray.fromPackedBytes(composite, vals.length);

        // Trigger ensureBytes() via get()
        assertEquals(vals[0], pfa.get(0), 0.0f);

        float mutated = 7.25f;
        writeFloatLE(segA, 0, mutated);

        assertEquals(vals[0], pfa.get(0), 0.0f);
        assertThat(pfa.asFloatArray(), equalTo(vals));
    }

    // 'cached' is used
    public void testAsFloatArrayIsCachedSameInstance() {
        float[] vals = new float[] { 1f, 2f, 3f };
        PackedFloatArray pfa = PackedFloatArray.fromPackedArray(packLE(vals), vals.length);

        float[] a = pfa.asFloatArray();
        float[] b = pfa.asFloatArray();

        assertThat(b, sameInstance(a));
        assertThat(a, equalTo(vals));
    }

    public void testWriteReadRoundTrip() throws Exception {
        float[] vals = new float[] { 10.5f, -2.25f, 0f, 123.75f };
        PackedFloatArray pfa = PackedFloatArray.fromPackedArray(packLE(vals), vals.length);

        // write payload
        BytesStreamOutput out = new BytesStreamOutput();
        pfa.writePayloadTo(out);

        // read payload back
        StreamInput in = out.bytes().streamInput();
        int dim = in.readVInt();
        assertThat(dim, equalTo(vals.length));
        PackedFloatArray read = PackedFloatArray.readBodyFrom(in, vals.length);

        assertThat(read.asFloatArray(), equalTo(vals));
    }

    public void testValidateThrowsOnBadLength() {
        byte[] bad = new byte[7]; // not divisible by 4, and not dim*4
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PackedFloatArray.fromPackedArray(bad, 2));
        assertThat(e.getMessage(), containsString("Bad packed float length"));
    }

    public void testGetBoundsChecks() {
        float[] vals = new float[] { 1f, 2f };
        PackedFloatArray pfa = PackedFloatArray.fromPackedArray(packLE(vals), vals.length);

        expectThrows(IndexOutOfBoundsException.class, () -> pfa.get(-1));
        expectThrows(IndexOutOfBoundsException.class, () -> pfa.get(2));
    }

    // ---- helpers ----

    private static byte[] packLE(float[] vals) {
        ByteBuffer bb = ByteBuffer.allocate(vals.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vals) {
            bb.putFloat(v);
        }
        return bb.array();
    }

    private static void writeFloatLE(byte[] arr, int off, float v) {
        int bits = Float.floatToRawIntBits(v);
        arr[off] = (byte) bits;
        arr[off + 1] = (byte) (bits >>> 8);
        arr[off + 2] = (byte) (bits >>> 16);
        arr[off + 3] = (byte) (bits >>> 24);
    }
}
