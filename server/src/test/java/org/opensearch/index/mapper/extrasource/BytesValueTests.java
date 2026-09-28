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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class BytesValueTests extends OpenSearchTestCase {

    public void testTypeAndSize() {
        BytesValue v = new BytesValue(new BytesArray(new byte[] { 1, 2, 3, 4 }));

        assertThat(v.type(), is(ExtraFieldValue.Type.BYTES));
        assertThat(v.size(), is(4));
        assertThat(v.bytes().length(), is(4));
    }

    public void testWriteBodyToReadBodyFromRoundTrip_bytesArray() throws Exception {
        BytesReference ref = new BytesArray(new byte[] { 9, 8, 7 });

        BytesValue v = new BytesValue(ref);

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeBodyTo(out);

        StreamInput in = out.bytes().streamInput();
        BytesValue read = BytesValue.readBodyFrom(in);

        assertThat(read.type(), is(ExtraFieldValue.Type.BYTES));
        assertThat(read.size(), is(3));
        BytesRef bytesRef = read.bytes().toBytesRef();
        assertThat(bytesRef.length, is(3));
        assertThat(bytesRef.bytes[bytesRef.offset], is((byte) 9));
    }

    public void testWriteBodyToReadBodyFromRoundTrip_composite() throws Exception {
        byte[] all = new byte[] { 1, 2, 3, 4, 5, 6 };
        BytesReference a = new BytesArray(all, 0, 2);
        BytesReference b = new BytesArray(all, 2, 4);
        BytesReference composite = CompositeBytesReference.of(a, b);

        BytesValue v = new BytesValue(composite);

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeBodyTo(out);

        StreamInput in = out.bytes().streamInput();
        BytesValue read = BytesValue.readBodyFrom(in);

        assertThat(read.size(), is(6));
        assertThat(BytesReference.toBytes(read.bytes()), equalTo(all));
    }

    public void testWriteToReadFromRoundTrip_viaExtraFieldValueDispatch() throws Exception {
        BytesValue v = new BytesValue(new BytesArray(new byte[] { 10, 11 }));

        BytesStreamOutput out = new BytesStreamOutput();
        v.writeTo(out); // writes [typeId][body]

        StreamInput in = out.bytes().streamInput();
        ExtraFieldValue read = ExtraFieldValue.readFrom(in);

        assertThat(read, instanceOf(BytesValue.class));
        BytesValue bv = (BytesValue) read;
        assertThat(bv.size(), is(2));
        assertThat(BytesReference.toBytes(bv.bytes()), equalTo(new byte[] { 10, 11 }));
    }
}
