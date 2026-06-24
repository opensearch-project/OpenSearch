/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.Index;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ModifyDataStreamsIT extends DataStreamTestCase {

    private List<String> backingIndexNames(String dataStreamName) throws Exception {
        DataStream dataStream = getDataStreams(dataStreamName).getDataStreams().get(0).getDataStream();
        return dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList());
    }

    public void testRemoveThenAddBackingIndex() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");
        rolloverDataStream("logs-demo");

        // two backing indices, write index is generation 2
        assertThat(backingIndexNames("logs-demo"), containsInAnyOrder(".ds-logs-demo-000001", ".ds-logs-demo-000002"));

        // remove the (non-write) first backing index; it becomes a standalone index again
        modifyDataStream(DataStreamAction.removeBackingIndex("logs-demo", ".ds-logs-demo-000001"));
        assertThat(backingIndexNames("logs-demo"), containsInAnyOrder(".ds-logs-demo-000002"));
        IndexMetadata removed = clusterAdmin().prepareState().get().getState().metadata().index(".ds-logs-demo-000001");
        assertNotNull(removed);
        assertFalse(IndexMetadata.INDEX_HIDDEN_SETTING.get(removed.getSettings()));

        // add it back; the data stream references it again and it is hidden once more
        modifyDataStream(DataStreamAction.addBackingIndex("logs-demo", ".ds-logs-demo-000001"));
        assertThat(backingIndexNames("logs-demo"), containsInAnyOrder(".ds-logs-demo-000001", ".ds-logs-demo-000002"));
        IndexMetadata added = clusterAdmin().prepareState().get().getState().metadata().index(".ds-logs-demo-000001");
        assertTrue(IndexMetadata.INDEX_HIDDEN_SETTING.get(added.getSettings()));

        // generation is unchanged by add/remove of backing indices
        assertThat(getDataStreams("logs-demo").getDataStreams().get(0).getDataStream().getGeneration(), equalTo(2L));
    }

    public void testAddStandaloneIndexAsBackingIndex() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");

        // an externally created index (arbitrary name) with a @timestamp field
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("restored-001")
                .setMapping("@timestamp", "type=date")
                .get()
        );

        modifyDataStream(DataStreamAction.addBackingIndex("logs-demo", "restored-001"));

        // the arbitrarily named index is now a backing index (allowed for non-write backing indices)
        assertThat(backingIndexNames("logs-demo"), hasItem("restored-001"));
        // and it was auto-adapted to be hidden
        IndexMetadata added = clusterAdmin().prepareState().get().getState().metadata().index("restored-001");
        assertTrue(IndexMetadata.INDEX_HIDDEN_SETTING.get(added.getSettings()));
    }

    public void testRemoveWriteIndexFails() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");

        // .ds-logs-demo-000001 is the only (and therefore write) index
        ExecutionException e = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .modifyDataStream(new ModifyDataStreamsAction.Request(List.of(DataStreamAction.removeBackingIndex("logs-demo", ".ds-logs-demo-000001"))))
                .get()
        );
        // the request may be handled on a remote cluster-manager node, in which case the cause is wrapped in a
        // RemoteTransportException; unwrap it before asserting on the underlying IllegalArgumentException
        Throwable cause = ExceptionsHelper.unwrapCause(e.getCause());
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(), containsString("is the write index"));

        // the data stream is unchanged
        assertThat(backingIndexNames("logs-demo"), containsInAnyOrder(".ds-logs-demo-000001"));
        assertThat(backingIndexNames("logs-demo"), not(hasItem("nonexistent")));
    }
}
