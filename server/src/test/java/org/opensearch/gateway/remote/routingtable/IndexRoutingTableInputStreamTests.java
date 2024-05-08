/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.seqno.ReplicationTrackerTestCase;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class IndexRoutingTableInputStreamTests extends ReplicationTrackerTestCase {

    public void testRoutingTableInputStream() throws IOException {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexShardRoutingTables -> {
            try {
                logger.info("IndexShardRoutingTables: {}", indexShardRoutingTables);
                InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexShardRoutingTables,
                    initialRoutingTable.version(), Version.CURRENT);

                IndexRoutingTableInputStreamReader reader = new IndexRoutingTableInputStreamReader(indexRoutingStream);
                Map<String, IndexShardRoutingTable> indexShardRoutingTableMap = reader.read();

                logger.info("indexShardRoutingTableMap: {}", indexShardRoutingTableMap);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
