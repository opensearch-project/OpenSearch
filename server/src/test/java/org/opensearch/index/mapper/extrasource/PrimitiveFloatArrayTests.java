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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PrimitiveFloatArrayTests extends OpenSearchTestCase {

    public void testBasics_dimensionGetAsArray() {
        float[] vals = new float[] { 10.5f, -2.25f, 0f, 123.75f };
        PrimitiveFloatArray pfa = new PrimitiveFloatArray(vals);

        assertThat(pfa.dimension(), is(vals.length));
        assertThat(pfa.isPackedLE(), is(false));

        for (int i = 0; i < vals.length; i++) {
            assertEquals(vals[i], pfa.get(i), 0.0f);
        }

        // asFloatArray returns the same array instance (no copy)
        assertThat(pfa.asFloatArray(), sameInstance(vals));
    }

    public void testPackedBytesThrows() {
        PrimitiveFloatArray pfa = new PrimitiveFloatArray(new float[] { 1.0f });

        IllegalStateException e = expectThrows(IllegalStateException.class, pfa::packedBytes);
        assertThat(e.getMessage(), containsString("Not packed"));
    }

    public void testWriteReadRoundTrip() throws Exception {
        float[] vals = new float[] { 1.25f, -3.75f, 1000.0f };
        PrimitiveFloatArray pfa = new PrimitiveFloatArray(vals);

        BytesStreamOutput out = new BytesStreamOutput();
        pfa.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        PrimitiveFloatArray read = PrimitiveFloatArray.readBodyFrom(in);

        assertThat(read.dimension(), is(vals.length));
        assertThat(read.isPackedLE(), is(false));
        assertThat(read.dimension(), is(vals.length));
        assertThat(read.asFloatArray(), equalTo(vals));
    }

    public void testEmptyArray() throws Exception {
        float[] vals = new float[0];
        PrimitiveFloatArray pfa = new PrimitiveFloatArray(vals);

        assertThat(pfa.dimension(), is(0));
        assertThat(pfa.asFloatArray(), sameInstance(vals));

        BytesStreamOutput out = new BytesStreamOutput();
        pfa.writePayloadTo(out);

        StreamInput in = out.bytes().streamInput();
        PrimitiveFloatArray read = PrimitiveFloatArray.readBodyFrom(in);

        assertThat(read.dimension(), is(0));
        assertThat(read.asFloatArray().length, is(0));
    }
}
