/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RemoteMetadataRef;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.IndexMetadataUpdater;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdater;

import static org.opensearch.common.xcontent.XContentHelper.createParser;

public class IndexMetadataPersistenceService {

    public RemoteMetadataRef persist(IndexMetadataChangeEvent clusterChangedEvent) throws IOException {
        List<IndexMetadata> existingMetadata = new ArrayList<>();
        if (clusterChangedEvent.oldState != null) {
            existingMetadata = load(clusterChangedEvent.oldState);
        }
        Map<Index, IndexMetadata> allIndices = existingMetadata.stream().collect(Collectors.toMap(IndexMetadata::getIndex, item -> item));
        for (IndexMetadata indexMetadata : clusterChangedEvent.state) {
            allIndices.put(indexMetadata.getIndex(), indexMetadata);
        }
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        for (IndexMetadata indexMetadata : allIndices.values()) {
            IndexMetadata.FORMAT.toXContent(builder, indexMetadata);
        }
        builder.endObject();

        Path tempFile = Files.createTempFile("index-metadata", ".data");
        try (PrintStream out = new PrintStream(new FileOutputStream(tempFile.toFile()))) {
            out.print(builder.toString());
        }
        return new RemoteMetadataRef("index_metadata", tempFile.toAbsolutePath().toString());
    }


    public List<IndexMetadata> load(RemoteMetadataRef remoteMetadataRef) throws IOException {

        List<IndexMetadata> indexMetadata = new ArrayList<>();
        File dataFile = new File(remoteMetadataRef.arn);
        String content = Files.readString(dataFile.toPath());
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(content),
                MediaTypeRegistry.JSON
            )
        ) {
            parser.nextToken();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                indexMetadata.add(IndexMetadata.Builder.fromXContent(parser));
            }
        }
        return indexMetadata;
    }

    public static class IndexMetadataChangeEvent {
        private final String source;

        private final List<IndexMetadata> state;

        private final String event_type = "create_or_update";

        private final RemoteMetadataRef oldState;

        public IndexMetadataChangeEvent(String source, List<IndexMetadata> state, RemoteMetadataRef oldState) {
            this.source = source;
            this.state = state;
            this.oldState = oldState;
        }

        @Override
        public String toString() {
            return "IndexMetadataChangeEvent{" + "source='" + source + '\'' + ", state=" + state + '}';
        }
    }

    public static class RoutingTableChangeEvent {
        private final String source;

        private final List<RoutingTable> state;

        private final String event_type = "create_or_update";

        private final RemoteMetadataRef oldState;

        public RoutingTableChangeEvent(String source, List<RoutingTable> state, RemoteMetadataRef oldState) {
            this.source = source;
            this.state = state;
            this.oldState = oldState;
        }

        @Override
        public String toString() {
            return "IndexMetadataChangeEvent{" + "source='" + source + '\'' + ", state=" + state + '}';
        }
    }
}
