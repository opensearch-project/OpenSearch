/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.search.DatafusionReaderRegistry;

import java.io.IOException;
import java.util.Map;

/**
 * Information about DataFusion on a specific node
 */
public class NodeDataFusionInfo extends BaseNodeResponse implements ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(NodeDataFusionInfo.class);
    
    private final String dataFusionVersion;
    private final int activeReaderCount;
    private final int totalRefCount;
    private final Map<Long, DatafusionReaderRegistry.ReaderInfo> readerDetails;

    /**
     * Constructor for NodeDataFusionInfo.
     * @param node The discovery node.
     * @param dataFusionVersion The DataFusion version.
     */
    public NodeDataFusionInfo(DiscoveryNode node, String dataFusionVersion) {
        super(node);
        this.dataFusionVersion = dataFusionVersion;
        // Collect reader information from the registry
        DatafusionReaderRegistry registry = DatafusionReaderRegistry.getInstance();
        
        // Clean up any stale readers first
        int cleanedUp = registry.cleanupStaleReaders();
        if (cleanedUp > 0) {
            logger.info("Cleaned up {} stale readers before collecting info", cleanedUp);
        }
        
        this.activeReaderCount = registry.getActiveReaderCount();
        this.totalRefCount = registry.getTotalRefCount();
        this.readerDetails = registry.getAllReaderInfo();
        
        // Log reader reference counts when DataFusionInfo is called
        logger.info("DataFusion Info Request - Node: {}, Version: {}", node.getId(), dataFusionVersion);
        registry.logAllReaderRefCounts("[DataFusion Info Request]");
        logger.info("DataFusion Reader Statistics Summary - Active Readers: {}, Total RefCount: {}", 
            activeReaderCount, totalRefCount);
    }

    /**
     * Constructor for NodeDataFusionInfo from stream input.
     * @param in The stream input.
     * @throws IOException If an I/O error occurs.
     */
    public NodeDataFusionInfo(StreamInput in) throws IOException {
        super(in);
        this.dataFusionVersion = in.readString();
        this.activeReaderCount = in.readInt();
        this.totalRefCount = in.readInt();
        // Reader details are not serialized for transport, only collected locally
        this.readerDetails = null;
    }

    /**
     * Writes the node info to the stream output.
     * @param out The stream output.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(dataFusionVersion);
        out.writeInt(activeReaderCount);
        out.writeInt(totalRefCount);
        // Reader details are not serialized for transport
    }

    /**
     * Converts the node info to XContent.
     * @param builder The XContent builder.
     * @param params The parameters.
     * @return The XContent builder.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("data_fusion_info");
        builder.field("datafusion_version", dataFusionVersion);
        
        // Add reader statistics
        builder.startObject("reader_statistics");
        builder.field("active_reader_count", activeReaderCount);
        builder.field("total_ref_count", totalRefCount);
        
        // Add detailed reader information if available
        if (readerDetails != null && !readerDetails.isEmpty()) {
            builder.startArray("readers");
            for (Map.Entry<Long, DatafusionReaderRegistry.ReaderInfo> entry : readerDetails.entrySet()) {
                DatafusionReaderRegistry.ReaderInfo info = entry.getValue();
                builder.startObject();
                builder.field("reader_id", info.readerId);
                builder.field("shard_id", info.shardId);
                builder.field("ref_count", info.getRefCount());
                builder.field("directory_path", info.directoryPath);
                builder.field("age_ms", System.currentTimeMillis() - info.registrationTime);
                builder.endObject();
            }
            builder.endArray();
        }
        
        builder.endObject(); // reader_statistics
        builder.endObject(); // data_fusion_info
        builder.endObject();
        return builder;
    }

    /**
     * Gets the DataFusion version.
     * @return The DataFusion version.
     */
    public String getDataFusionVersion() {
        return dataFusionVersion;
    }
    
    /**
     * Gets the active reader count.
     * @return The active reader count.
     */
    public int getActiveReaderCount() {
        return activeReaderCount;
    }
    
    /**
     * Gets the total reference count.
     * @return The total reference count.
     */
    public int getTotalRefCount() {
        return totalRefCount;
    }
}
