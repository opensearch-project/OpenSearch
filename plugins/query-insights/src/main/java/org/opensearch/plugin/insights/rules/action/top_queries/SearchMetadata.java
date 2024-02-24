/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SearchTaskMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Task resource usage information with minimal information about the task
 * <p>
 * Writeable TaskResourceInfo objects are used to represent resource usage
 * information of running tasks, which can be propagated to coordinator node
 * to infer query-level resource usage
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.1.0")
public class SearchMetadata extends BaseNodeResponse implements Writeable, ToXContentObject {
    public List<SearchQueryRecord> queryRecordList;
    public List<SearchTaskMetadata> taskMetadataList;
    public Map<Long, Integer> taskStatusMap;

    /**
     * Create the TopQueries Object from StreamInput
     * @param in A {@link StreamInput} object.
     * @throws IOException IOException
     */
    public SearchMetadata(final StreamInput in) throws IOException {
        super(in);
        queryRecordList = in.readList(SearchQueryRecord::new);
        taskMetadataList = in.readList(SearchTaskMetadata::new);
        taskStatusMap = in.readMap(StreamInput::readLong, StreamInput::readInt);
    }

    /**
     * Create the TopQueries Object
     * @param node A node that is part of the cluster.
     * @param taskMetadataList A list of SearchQueryRecord associated in this TopQueries.
     */
    public SearchMetadata(final DiscoveryNode node, List<SearchQueryRecord> queryRecordList, List<SearchTaskMetadata> taskMetadataList, Map<Long, Integer> taskStatusMap) {
        super(node);
        this.queryRecordList = queryRecordList;
        this.taskMetadataList = taskMetadataList;
        this.taskStatusMap = taskStatusMap;

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(queryRecordList);
        out.writeList(taskMetadataList);
        out.writeMap(taskStatusMap, StreamOutput::writeLong, StreamOutput::writeInt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (taskMetadataList != null) {
            for (SearchTaskMetadata metadata : taskMetadataList) {
                metadata.toXContent(builder, params);
            }
        }
        return builder.endObject();
    }
}
