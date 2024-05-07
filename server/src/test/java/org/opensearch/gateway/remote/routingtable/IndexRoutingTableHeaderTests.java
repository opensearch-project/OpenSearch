/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.InputStreamDataInput;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class IndexRoutingTableHeaderTests extends OpenSearchTestCase {

    public void testWrite() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        IndexRoutingTableHeader header = new IndexRoutingTableHeader(1, "dummyIndex", Version.V_3_0_0);
        header.write(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes);
        CodecUtil.checkHeader(new InputStreamDataInput(in),IndexRoutingTableHeader.INDEX_ROUTING_HEADER_CODEC, IndexRoutingTableHeader.INITIAL_VERSION, IndexRoutingTableHeader.CURRENT_VERSION );
        assertEquals(1, in.readLong());
        assertEquals(Version.V_3_0_0.id, in.readInt());
        assertEquals("dummyIndex", in.readString());
    }

}
