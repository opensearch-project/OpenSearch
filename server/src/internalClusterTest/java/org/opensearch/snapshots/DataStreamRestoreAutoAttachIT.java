/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.opensearch.action.admin.indices.datastream.DataStreamAction;
import org.opensearch.action.admin.indices.datastream.GetDataStreamAction;
import org.opensearch.action.admin.indices.datastream.ModifyDataStreamsAction;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.settings.Settings;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that restoring a data stream backing index attaches it to a pre-existing data stream of the same name in the
 * same cluster-state update when {@code attach_to_data_stream} is set, advancing the stream generation when the
 * attached index has a higher counter than the current write index.
 */
public class DataStreamRestoreAutoAttachIT extends AbstractSnapshotIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String DS = "logs-attach";

    private void createTemplate() throws Exception {
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of("logs-*"),
            new Template(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build(), null, null),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField("@timestamp"))
        );
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("ds-template");
        request.indexTemplate(template);
        assertAcked(client().execute(PutComposableIndexTemplateAction.INSTANCE, request).get());
    }

    private List<String> backingIndices() throws Exception {
        return client().admin()
            .indices()
            .getDataStreams(new GetDataStreamAction.Request(new String[] { DS }))
            .get()
            .getDataStreams()
            .get(0)
            .getDataStream()
            .getIndices()
            .stream()
            .map(i -> i.getName())
            .collect(Collectors.toList());
    }

    private long generation() throws Exception {
        return client().admin()
            .indices()
            .getDataStreams(new GetDataStreamAction.Request(new String[] { DS }))
            .get()
            .getDataStreams()
            .get(0)
            .getDataStream()
            .getGeneration();
    }

    public void testRestoreReattachesDetachedBackingIndex() throws Exception {
        createRepository(REPO, "fs");
        createTemplate();
        assertAcked(client().admin().indices().createDataStream(new CreateDataStreamAction.Request(DS)).get());
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(DS, null)).get().isRolledOver(), equalTo(true)); // gen 2
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(DS, null)).get().isRolledOver(), equalTo(true)); // gen 3
        assertThat(generation(), equalTo(3L));

        // Snapshot the whole data stream (includes the gen-3 backing index).
        createSnapshot(REPO, "snap", List.of(DS));

        // Roll once more so the gen-3 index is no longer the write index, then detach and delete it to model a backing
        // index that is missing locally but present in the snapshot.
        String gen3 = DataStream.getDefaultBackingIndexName(DS, 3);
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(DS, null)).get().isRolledOver(), equalTo(true)); // gen 4
        assertAcked(
            client().execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(List.of(DataStreamAction.removeBackingIndex(DS, gen3)))
            ).get()
        );
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(gen3)).get());
        assertThat(backingIndices().contains(gen3), equalTo(false));

        // Restore just the detached backing index; with attach_to_data_stream it re-attaches to the stream.
        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, "snap")
            .setIndices(gen3)
            .setAttachToDataStream(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().successfulShards(), equalTo(restore.getRestoreInfo().totalShards()));

        // The restored index is back in the stream, in generation order.
        assertThat(backingIndices().contains(gen3), equalTo(true));
        assertThat(
            backingIndices(),
            contains(
                DataStream.getDefaultBackingIndexName(DS, 1),
                DataStream.getDefaultBackingIndexName(DS, 2),
                DataStream.getDefaultBackingIndexName(DS, 3),
                DataStream.getDefaultBackingIndexName(DS, 4)
            )
        );
        // Generation stays at 4 (gen-3 is not the write index); reattaching a lower-counter index does not change it.
        assertThat(generation(), equalTo(4L));
    }

    public void testRestoreWithRenameAdvancesGenerationOfRenamedStream() throws Exception {
        // Exercises the two things a unit test of attachRestoredBackingIndices cannot: that the restore pipeline feeds
        // the post-rename index name into attach, and that attaching a higher-generation index passes cluster-state
        // validation atomically and leaves rollover working. The source is a generation ahead of the target.
        createRepository(REPO, "fs");
        createTemplate();

        // Source stream rolled to generation 3, snapshotted; its gen-3 index is the write index.
        String source = "logs-source";
        assertAcked(client().admin().indices().createDataStream(new CreateDataStreamAction.Request(source)).get());
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(source, null)).get().isRolledOver(), equalTo(true)); // 2
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(source, null)).get().isRolledOver(), equalTo(true)); // 3
        createSnapshot(REPO, "snap", List.of(source));

        // Target stream logs-attach is a generation behind at 2 (backing [1, 2]).
        assertAcked(client().admin().indices().createDataStream(new CreateDataStreamAction.Request(DS)).get());
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(DS, null)).get().isRolledOver(), equalTo(true)); // 2
        assertThat(generation(), equalTo(2L));

        // Restore the source's gen-3 index, renamed into the target stream. Auto-attach keys off the post-rename name,
        // and the higher counter advances the target generation to 3, making it the new write index.
        String targetGen3 = DataStream.getDefaultBackingIndexName(DS, 3);
        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, "snap")
            .setIndices(DataStream.getDefaultBackingIndexName(source, 3))
            .setRenamePattern(source)
            .setRenameReplacement(DS)
            .setAttachToDataStream(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().successfulShards(), equalTo(restore.getRestoreInfo().totalShards()));

        assertTrue(backingIndices().contains(targetGen3));
        assertThat(generation(), equalTo(3L));
        assertThat(backingIndices().get(backingIndices().size() - 1), equalTo(targetGen3));
        // A subsequent rollover advances off the new write index, confirming generation stayed in sync.
        assertThat(client().admin().indices().rolloverIndex(new RolloverRequest(DS, null)).get().isRolledOver(), equalTo(true));
        assertThat(generation(), equalTo(4L));
    }
}
