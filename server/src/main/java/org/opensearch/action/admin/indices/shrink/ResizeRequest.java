/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.shrink;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request class to shrink an index into a single shard
 *
 * @opensearch.internal
 */
public class ResizeRequest extends AcknowledgedRequest<ResizeRequest> implements IndicesRequest, ToXContentObject {

    public static final ObjectParser<ResizeRequest, Void> PARSER = new ObjectParser<>("resize_request");
    private static final ParseField MAX_SHARD_SIZE = new ParseField("max_shard_size");

    static {
        PARSER.declareField(
            (parser, request, context) -> request.getTargetIndexRequest().settings(parser.map()),
            new ParseField("settings"),
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            (parser, request, context) -> request.getTargetIndexRequest().aliases(parser.map()),
            new ParseField("aliases"),
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            ResizeRequest::setMaxShardSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SHARD_SIZE.getPreferredName()),
            MAX_SHARD_SIZE,
            ObjectParser.ValueType.STRING
        );
    }

    private CreateIndexRequest targetIndexRequest;
    private String sourceIndex;
    private ResizeType type = ResizeType.SHRINK;
    private Boolean copySettings = true;
    private ByteSizeValue maxShardSize;
    private boolean shouldStoreResult;

    public ResizeRequest(StreamInput in) throws IOException {
        super(in);
        targetIndexRequest = new CreateIndexRequest(in);
        sourceIndex = in.readString();
        type = in.readEnum(ResizeType.class);
        copySettings = in.readOptionalBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_5_0)) {
            maxShardSize = in.readOptionalWriteable(ByteSizeValue::new);
        }
    }

    ResizeRequest() {}

    public ResizeRequest(String targetIndex, String sourceIndex) {
        this.targetIndexRequest = new CreateIndexRequest(targetIndex);
        this.sourceIndex = sourceIndex;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = targetIndexRequest == null ? null : targetIndexRequest.validate();
        if (sourceIndex == null) {
            validationException = addValidationError("source index is missing", validationException);
        }
        if (targetIndexRequest == null) {
            validationException = addValidationError("target index request is missing", validationException);
        }
        if (targetIndexRequest.settings().getByPrefix("index.sort.").isEmpty() == false) {
            validationException = addValidationError("can't override index sort when resizing an index", validationException);
        }
        if (IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.exists(targetIndexRequest.settings())) {
            validationException = addValidationError(
                "cannot provide a routing partition size value when resizing an index",
                validationException
            );
        }
        if (type == ResizeType.SPLIT && IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexRequest.settings()) == false) {
            validationException = addValidationError("index.number_of_shards is required for split operations", validationException);
        }

        // max_shard_size is only supported for shrink
        if (type != ResizeType.SHRINK && maxShardSize != null) {
            validationException = addValidationError("Unsupported parameter [max_shard_size]", validationException);
        }
        // max_shard_size conflicts with the index.number_of_shards setting
        if (type == ResizeType.SHRINK
            && IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexRequest.settings())
            && maxShardSize != null) {
            validationException = addValidationError(
                "Cannot set max_shard_size and index.number_of_shards at the same time!",
                validationException
            );
        }
        if (maxShardSize != null && maxShardSize.getBytes() <= 0) {
            validationException = addValidationError("max_shard_size must be greater than 0", validationException);
        }
        // Check target index's settings, if `index.blocks.read_only` is `true`, the target index's metadata writes will be disabled
        // and then cause the new shards to be unassigned.
        if (IndexMetadata.INDEX_READ_ONLY_SETTING.get(targetIndexRequest.settings()) == true) {
            validationException = addValidationError(
                "target index ["
                    + targetIndexRequest.index()
                    + "] will be blocked by [index.blocks.read_only=true], this will disable metadata writes and cause the shards to be unassigned",
                validationException
            );
        }

        // Check target index's settings, if `index.blocks.metadata` is `true`, the target index's metadata writes will be disabled
        // and then cause the new shards to be unassigned.
        if (IndexMetadata.INDEX_BLOCKS_METADATA_SETTING.get(targetIndexRequest.settings()) == true) {
            validationException = addValidationError(
                "target index ["
                    + targetIndexRequest.index()
                    + "] will be blocked by [index.blocks.metadata=true], this will disable metadata writes and cause the shards to be unassigned",
                validationException
            );
        }
        assert copySettings == null || copySettings;
        return validationException;
    }

    public void setSourceIndex(String index) {
        this.sourceIndex = index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        targetIndexRequest.writeTo(out);
        out.writeString(sourceIndex);
        out.writeEnum(type);
        out.writeOptionalBoolean(copySettings);
        if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
            out.writeOptionalWriteable(maxShardSize);
        }
    }

    @Override
    public String[] indices() {
        return new String[] { sourceIndex };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandOpen();
    }

    public void setTargetIndex(CreateIndexRequest targetIndexRequest) {
        this.targetIndexRequest = Objects.requireNonNull(targetIndexRequest, "target index request must not be null");
    }

    /**
     * Returns the {@link CreateIndexRequest} for the shrink index
     */
    public CreateIndexRequest getTargetIndexRequest() {
        return targetIndexRequest;
    }

    /**
     * Returns the source index name
     */
    public String getSourceIndex() {
        return sourceIndex;
    }

    /**
     * Sets the number of shard copies that should be active for creation of the
     * new shrunken index to return. Defaults to {@link ActiveShardCount#DEFAULT}, which will
     * wait for one shard copy (the primary) to become active. Set this value to
     * {@link ActiveShardCount#ALL} to wait for all shards (primary and all replicas) to be active
     * before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link ResizeResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public void setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.getTargetIndexRequest().waitForActiveShards(waitForActiveShards);
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public void setWaitForActiveShards(final int waitForActiveShards) {
        setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * The type of the resize operation
     */
    public void setResizeType(ResizeType type) {
        this.type = Objects.requireNonNull(type);
    }

    /**
     * Returns the type of the resize operation
     */
    public ResizeType getResizeType() {
        return type;
    }

    public void setCopySettings(final Boolean copySettings) {
        if (copySettings != null && copySettings == false) {
            throw new IllegalArgumentException("[copySettings] can not be explicitly set to [false]");
        }
        this.copySettings = copySettings;
    }

    public Boolean getCopySettings() {
        return copySettings;
    }

    /**
     * Sets the maximum size of a primary shard in the new shrunken index.
     * This parameter can be used to calculate the lowest factor of the source index's shards number
     * which satisfies the maximum shard size requirement.
     *
     * @param maxShardSize the maximum size of a primary shard in the new shrunken index
     */
    public void setMaxShardSize(ByteSizeValue maxShardSize) {
        this.maxShardSize = maxShardSize;
    }

    /**
     * Returns the maximum size of a primary shard in the new shrunken index.
     */
    public ByteSizeValue getMaxShardSize() {
        return maxShardSize;
    }

    /**
     * Should this task store its result after it has finished?
     */
    public void setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(CreateIndexRequest.SETTINGS.getPreferredName());
            {
                targetIndexRequest.settings().toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject(CreateIndexRequest.ALIASES.getPreferredName());
            {
                for (Alias alias : targetIndexRequest.aliases()) {
                    alias.toXContent(builder, params);
                }
            }
            builder.endObject();
            if (maxShardSize != null) {
                builder.field(MAX_SHARD_SIZE.getPreferredName(), maxShardSize);
            }
        }
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        PARSER.parse(parser, this, null);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        switch (getResizeType()) {
            case SPLIT:
                b.append("split from");
                break;
            case CLONE:
                b.append("clone from");
                break;
            default:
                b.append("shrink from");
        }
        b.append(" [").append(sourceIndex).append("]");
        b.append(" to [").append(getTargetIndexRequest().index()).append(']');
        return b.toString();
    }

    @Override
    public String getDescription() {
        return this.toString();
    }
}
