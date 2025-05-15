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

package org.opensearch.action.bulk;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.throwUnknownField;

/**
 * Represents a single item response for an action executed as part of the bulk API. Holds the index/type/id
 * of the relevant action, and if it has failed or not (with the failure message in case it failed).
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class BulkItemResponse implements Writeable, StatusToXContentObject {

    private static final String _INDEX = "_index";
    private static final String _ID = "_id";
    private static final String STATUS = "status";
    private static final String ERROR = "error";

    @Override
    public RestStatus status() {
        return failure == null ? response.status() : failure.getStatus();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(opType.getLowercase());
        if (failure == null) {
            response.innerToXContent(builder, params);
            builder.field(STATUS, response.status().getStatus());
        } else {
            builder.field(_INDEX, failure.getIndex());
            builder.field(_ID, failure.getId());
            builder.field(STATUS, failure.getStatus().getStatus());
            builder.startObject(ERROR);
            OpenSearchException.generateThrowableXContent(builder, params, failure.getCause());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Reads a {@link BulkItemResponse} from a {@link XContentParser}.
     *
     * @param parser the {@link XContentParser}
     * @param id the id to assign to the parsed {@link BulkItemResponse}. It is usually the index of
     *           the item in the {@link BulkResponse#getItems} array.
     */
    public static BulkItemResponse fromXContent(XContentParser parser, int id) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String currentFieldName = parser.currentName();
        token = parser.nextToken();

        final OpType opType = OpType.fromString(currentFieldName);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        DocWriteResponse.Builder builder = null;
        CheckedConsumer<XContentParser, IOException> itemParser = null;

        if (opType == OpType.INDEX || opType == OpType.CREATE) {
            final IndexResponse.Builder indexResponseBuilder = new IndexResponse.Builder();
            builder = indexResponseBuilder;
            itemParser = (indexParser) -> IndexResponse.parseXContentFields(indexParser, indexResponseBuilder);

        } else if (opType == OpType.UPDATE) {
            final UpdateResponse.Builder updateResponseBuilder = new UpdateResponse.Builder();
            builder = updateResponseBuilder;
            itemParser = (updateParser) -> UpdateResponse.parseXContentFields(updateParser, updateResponseBuilder);

        } else if (opType == OpType.DELETE) {
            final DeleteResponse.Builder deleteResponseBuilder = new DeleteResponse.Builder();
            builder = deleteResponseBuilder;
            itemParser = (deleteParser) -> DeleteResponse.parseXContentFields(deleteParser, deleteResponseBuilder);
        } else {
            throwUnknownField(currentFieldName, parser.getTokenLocation());
        }

        RestStatus status = null;
        OpenSearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }

            if (ERROR.equals(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    exception = OpenSearchException.fromXContent(parser);
                }
            } else if (STATUS.equals(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                itemParser.accept(parser);
            }
        }

        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.END_OBJECT, token, parser);

        BulkItemResponse bulkItemResponse;
        if (exception != null) {
            Failure failure = new Failure(builder.getShardId().getIndexName(), builder.getId(), exception, status);
            bulkItemResponse = new BulkItemResponse(id, opType, failure);
        } else {
            bulkItemResponse = new BulkItemResponse(id, opType, builder.build());
        }
        return bulkItemResponse;
    }

    /**
     * Represents a failure.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Failure implements Writeable, ToXContentFragment {
        public static final String INDEX_FIELD = "index";
        public static final String ID_FIELD = "id";
        public static final String CAUSE_FIELD = "cause";
        public static final String STATUS_FIELD = "status";

        private final String index;
        private final String id;
        private final Exception cause;
        private final RestStatus status;
        private final long seqNo;
        private final long term;
        private final boolean aborted;
        private final FailureSource source;

        /**
         * The source of the failure, denotes which step has failure during the bulk processing.
         */
        @PublicApi(since = "3.0.0")
        public enum FailureSource {
            UNKNOWN((byte) 0),
            // Pipeline execution failure
            PIPELINE((byte) 1),
            VALIDATION((byte) 2),
            WRITE_PROCESSING((byte) 3);

            private final byte sourceType;

            FailureSource(byte sourceType) {
                this.sourceType = sourceType;
            }

            public byte getSourceType() {
                return sourceType;
            }

            public static FailureSource fromSourceType(byte sourceType) {
                return switch (sourceType) {
                    case 0 -> UNKNOWN;
                    case 1 -> PIPELINE;
                    case 2 -> VALIDATION;
                    case 3 -> WRITE_PROCESSING;
                    default -> throw new IllegalArgumentException("Unknown failure source: [" + sourceType + "]");
                };
            }
        }

        public static final ConstructingObjectParser<Failure, Void> PARSER = new ConstructingObjectParser<>(
            "bulk_failures",
            true,
            a -> new Failure((String) a[0], (String) a[1], (Exception) a[2], RestStatus.fromCode((int) a[3]))
        );
        static {
            PARSER.declareString(constructorArg(), new ParseField(INDEX_FIELD));
            PARSER.declareString(optionalConstructorArg(), new ParseField(ID_FIELD));
            PARSER.declareObject(constructorArg(), (p, c) -> OpenSearchException.fromXContent(p), new ParseField(CAUSE_FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(STATUS_FIELD));
        }

        /**
         * For write failures before operation was assigned a sequence number.
         * <p>
         * use @{link {@link #Failure(String, String, Exception, long, long)}}
         * to record operation sequence no with failure
         */
        public Failure(String index, String id, Exception cause) {
            this(
                index,
                id,
                cause,
                ExceptionsHelper.status(cause),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                false,
                FailureSource.UNKNOWN
            );
        }

        public Failure(String index, String id, Exception cause, FailureSource source) {
            this(
                index,
                id,
                cause,
                ExceptionsHelper.status(cause),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                false,
                source
            );
        }

        public Failure(String index, String id, Exception cause, boolean aborted) {
            this(
                index,
                id,
                cause,
                ExceptionsHelper.status(cause),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                aborted,
                FailureSource.UNKNOWN
            );
        }

        public Failure(String index, String id, Exception cause, RestStatus status) {
            this(
                index,
                id,
                cause,
                status,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                false,
                FailureSource.UNKNOWN
            );
        }

        /** For write failures after operation was assigned a sequence number. */
        public Failure(String index, String id, Exception cause, long seqNo, long term) {
            this(index, id, cause, ExceptionsHelper.status(cause), seqNo, term, false, FailureSource.UNKNOWN);
        }

        private Failure(
            String index,
            String id,
            Exception cause,
            RestStatus status,
            long seqNo,
            long term,
            boolean aborted,
            FailureSource source
        ) {
            this.index = index;
            this.id = id;
            this.cause = cause;
            this.status = status;
            this.seqNo = seqNo;
            this.term = term;
            this.aborted = aborted;
            this.source = source;
        }

        /**
         * Read from a stream.
         */
        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                in.readString();
                // can't make an assertion about type names here because too many tests still set their own
                // types bypassing various checks
            }
            id = in.readOptionalString();
            cause = in.readException();
            status = ExceptionsHelper.status(cause);
            seqNo = in.readZLong();
            term = in.readVLong();
            aborted = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
                source = FailureSource.fromSourceType(in.readByte());
            } else {
                source = FailureSource.UNKNOWN;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getVersion().before(Version.V_2_0_0)) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeOptionalString(id);
            out.writeException(cause);
            out.writeZLong(seqNo);
            out.writeVLong(term);
            out.writeBoolean(aborted);
            if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
                out.writeByte(source.getSourceType());
            }
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure message.
         */
        public String getMessage() {
            return this.cause.toString();
        }

        /**
         * The rest status.
         */
        public RestStatus getStatus() {
            return this.status;
        }

        /**
         * The actual cause of the failure.
         */
        public Exception getCause() {
            return cause;
        }

        /**
         * The operation sequence number generated by primary
         * NOTE: {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
         * indicates sequence number was not generated by primary
         */
        public long getSeqNo() {
            return seqNo;
        }

        /**
         * The operation primary term of the primary
         * NOTE: {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM}
         * indicates primary term was not assigned by primary
         */
        public long getTerm() {
            return term;
        }

        /**
         * Whether this failure is the result of an <em>abort</em>.
         * If {@code true}, the request to which this failure relates should never be retried, regardless of the {@link #getCause() cause}.
         * @see BulkItemRequest#abort(String, Exception)
         */
        public boolean isAborted() {
            return aborted;
        }

        public FailureSource getSource() {
            return source;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(INDEX_FIELD, index);
            if (id != null) {
                builder.field(ID_FIELD, id);
            }
            builder.startObject(CAUSE_FIELD);
            OpenSearchException.generateThrowableXContent(builder, params, cause);
            builder.endObject();
            builder.field(STATUS_FIELD, status.getStatus());
            return builder;
        }

        public static Failure fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }
    }

    private int id;

    private OpType opType;

    private DocWriteResponse response;

    private Failure failure;

    BulkItemResponse() {}

    BulkItemResponse(ShardId shardId, StreamInput in) throws IOException {
        id = in.readVInt();
        opType = OpType.fromId(in.readByte());

        byte type = in.readByte();
        if (type == 0) {
            response = new IndexResponse(shardId, in);
        } else if (type == 1) {
            response = new DeleteResponse(shardId, in);
        } else if (type == 3) { // make 3 instead of 2, because 2 is already in use for 'no responses'
            response = new UpdateResponse(shardId, in);
        } else if (type != 2) {
            throw new IllegalArgumentException("Unexpected type [" + type + "]");
        }

        if (in.readBoolean()) {
            failure = new Failure(in);
        }
    }

    BulkItemResponse(StreamInput in) throws IOException {
        id = in.readVInt();
        opType = OpType.fromId(in.readByte());

        byte type = in.readByte();
        if (type == 0) {
            response = new IndexResponse(in);
        } else if (type == 1) {
            response = new DeleteResponse(in);
        } else if (type == 3) { // make 3 instead of 2, because 2 is already in use for 'no responses'
            response = new UpdateResponse(in);
        } else if (type != 2) {
            throw new IllegalArgumentException("Unexpected type [" + type + "]");
        }

        if (in.readBoolean()) {
            failure = new Failure(in);
        }
    }

    public BulkItemResponse(int id, OpType opType, DocWriteResponse response) {
        this.id = id;
        this.response = response;
        this.opType = opType;
    }

    public BulkItemResponse(int id, OpType opType, Failure failure) {
        this.id = id;
        this.opType = opType;
        this.failure = failure;
    }

    /**
     * The numeric order of the item matching the same request order in the bulk request.
     */
    public int getItemId() {
        return id;
    }

    /**
     * The operation type ("index", "create" or "delete").
     */
    public OpType getOpType() {
        return this.opType;
    }

    /**
     * The index name of the action.
     */
    public String getIndex() {
        if (failure != null) {
            return failure.getIndex();
        }
        return response.getIndex();
    }

    /**
     * The id of the action.
     */
    public String getId() {
        if (failure != null) {
            return failure.getId();
        }
        return response.getId();
    }

    /**
     * The version of the action.
     */
    public long getVersion() {
        if (failure != null) {
            return -1;
        }
        return response.getVersion();
    }

    /**
     * The actual response ({@link IndexResponse} or {@link DeleteResponse}). {@code null} in
     * case of failure.
     */
    public <T extends DocWriteResponse> T getResponse() {
        return (T) response;
    }

    /**
     * Is this a failed execution of an operation.
     */
    public boolean isFailed() {
        return failure != null;
    }

    /**
     * The failure message, {@code null} if it did not fail.
     */
    public String getFailureMessage() {
        if (failure != null) {
            return failure.getMessage();
        }
        return null;
    }

    /**
     * The actual failure object if there was a failure.
     */
    public Failure getFailure() {
        return this.failure;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeByte(opType.getId());

        if (response == null) {
            out.writeByte((byte) 2);
        } else {
            writeResponseType(out);
            response.writeTo(out);
        }
        if (failure == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            failure.writeTo(out);
        }
    }

    public void writeThin(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeByte(opType.getId());

        if (response == null) {
            out.writeByte((byte) 2);
        } else {
            writeResponseType(out);
            response.writeThin(out);
        }
        if (failure == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            failure.writeTo(out);
        }
    }

    private void writeResponseType(StreamOutput out) throws IOException {
        if (response instanceof IndexResponse) {
            out.writeByte((byte) 0);
        } else if (response instanceof DeleteResponse) {
            out.writeByte((byte) 1);
        } else if (response instanceof UpdateResponse) {
            out.writeByte((byte) 3); // make 3 instead of 2, because 2 is already in use for 'no responses'
        } else {
            throw new IllegalStateException("Unexpected response type found [" + response.getClass() + "]");
        }
    }
}
