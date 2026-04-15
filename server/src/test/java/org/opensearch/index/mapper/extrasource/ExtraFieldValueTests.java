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
        PrimitiveFloatArray v = new PrimitiveFloatArray(new float[] { 10.5f, -2.25f });

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

    public void testUnknownTypeIdThrows() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();

        // write unknown type id, then some junk body
        out.writeByte((byte) 42);
        out.writeVInt(0);

        StreamInput in = out.bytes().streamInput();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ExtraFieldValue.readFrom(in));
        assertThat(e.getMessage(), containsString("Unknown ExtraFieldValue.Type id"));
    }
}
