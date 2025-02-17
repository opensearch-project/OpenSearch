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

package org.opensearch.action.update;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.get.GetResult;

import java.io.IOException;

import static org.opensearch.Version.V_3_0_0;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Transport response for updating an index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpdateResponse extends DocWriteResponse {

    private static final String GET = "get";

    private GetResult getResult;

    private final InternalEngine.WriteStrategy writeStrategy;

    public UpdateResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        if (in.readBoolean()) {
            getResult = new GetResult(in);
        }
        if (in.getVersion().onOrAfter(V_3_0_0) && in.readBoolean()) {
            if (in.readBoolean()) {
                this.writeStrategy = new InternalEngine.IndexingStrategy(in);
            } else {
                this.writeStrategy = new InternalEngine.DeletionStrategy(in);
            }
        } else {
            this.writeStrategy = null;
        }
    }

    public UpdateResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            getResult = new GetResult(in);
        }
        if (in.getVersion().onOrAfter(V_3_0_0) && in.readBoolean()) {
            if (in.readBoolean()) {
                this.writeStrategy = new InternalEngine.IndexingStrategy(in);
            } else {
                this.writeStrategy = new InternalEngine.DeletionStrategy(in);
            }
        } else {
            this.writeStrategy = null;
        }
    }

    /**
     * Constructor to be used when a update didn't translate in a write.
     * For example: update script with operation set to none
     */
    public UpdateResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        this(new ShardInfo(0, 0), shardId, id, seqNo, primaryTerm, version, result);
    }

    public UpdateResponse(ShardInfo shardInfo, ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        this(shardInfo, shardId, id, seqNo, primaryTerm, version, result, null);
    }

    public UpdateResponse(
        ShardInfo shardInfo,
        ShardId shardId,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        Result result,
        InternalEngine.WriteStrategy writeStrategy
    ) {
        super(shardId, id, seqNo, primaryTerm, version, result);
        setShardInfo(shardInfo);
        this.writeStrategy = writeStrategy;
    }

    public void setGetResult(GetResult getResult) {
        this.getResult = getResult;
    }

    public GetResult getGetResult() {
        return this.getResult;
    }

    @Override
    public RestStatus status() {
        return this.result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        writeGetResult(out);
        if (out.getVersion().onOrAfter(V_3_0_0)) {
            if (writeStrategy == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                if (writeStrategy instanceof InternalEngine.IndexingStrategy) {
                    out.writeBoolean(true);
                    ((InternalEngine.IndexingStrategy) writeStrategy).writeTo(out);
                } else {
                    out.writeBoolean(false);
                    ((InternalEngine.DeletionStrategy) writeStrategy).writeTo(out);
                }
            }
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeGetResult(out);
        if (out.getVersion().onOrAfter(V_3_0_0)) {
            if (writeStrategy == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                if (writeStrategy instanceof InternalEngine.IndexingStrategy) {
                    out.writeBoolean(true);
                    ((InternalEngine.IndexingStrategy) writeStrategy).writeTo(out);
                } else {
                    out.writeBoolean(false);
                    ((InternalEngine.DeletionStrategy) writeStrategy).writeTo(out);
                }
            }
        }
    }

    private void writeGetResult(StreamOutput out) throws IOException {
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerToXContent(builder, params);
        if (getGetResult() != null) {
            builder.startObject(GET);
            getGetResult().toXContentEmbedded(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public InternalEngine.WriteStrategy writeStrategy() {
        return writeStrategy;
    };

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("UpdateResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",seqNo=").append(getSeqNo());
        builder.append(",primaryTerm=").append(getPrimaryTerm());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    public static UpdateResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        Builder context = new Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();

        if (GET.equals(currentFieldName)) {
            if (token == XContentParser.Token.START_OBJECT) {
                context.setGetResult(GetResult.fromXContentEmbedded(parser));
            }
        } else {
            DocWriteResponse.parseInnerToXContent(parser, context);
        }
    }

    /**
     * Builder class for {@link UpdateResponse}. This builder is usually used during xcontent parsing to
     * temporarily store the parsed values, then the {@link DocWriteResponse.Builder#build()} method is called to
     * instantiate the {@link UpdateResponse}.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder extends DocWriteResponse.Builder {

        private GetResult getResult = null;

        public void setGetResult(GetResult getResult) {
            this.getResult = getResult;
        }

        @Override
        public UpdateResponse build() {
            UpdateResponse update;
            if (shardInfo != null) {
                update = new UpdateResponse(shardInfo, shardId, id, seqNo, primaryTerm, version, result);
            } else {
                update = new UpdateResponse(shardId, id, seqNo, primaryTerm, version, result);
            }
            if (getResult != null) {
                update.setGetResult(
                    new GetResult(
                        update.getIndex(),
                        update.getId(),
                        getResult.getSeqNo(),
                        getResult.getPrimaryTerm(),
                        update.getVersion(),
                        getResult.isExists(),
                        getResult.internalSourceRef(),
                        getResult.getDocumentFields(),
                        getResult.getMetadataFields()
                    )
                );
            }
            update.setForcedRefresh(forcedRefresh);
            return update;
        }
    }
}
