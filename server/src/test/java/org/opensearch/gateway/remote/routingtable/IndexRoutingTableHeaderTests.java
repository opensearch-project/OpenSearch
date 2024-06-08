/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class IndexRoutingTableHeaderTests extends OpenSearchTestCase {

    public void testIndexRoutingTableHeader() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        IndexRoutingTableHeader header = new IndexRoutingTableHeader(indexName);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            header.writeTo(out);

            BytesStreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes);
            IndexRoutingTableHeader headerRead = new IndexRoutingTableHeader(in);
            assertEquals(indexName, headerRead.getIndexName());

        }
    }

}
