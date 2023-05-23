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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch;

import org.opensearch.action.support.replication.ReplicationOperation;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.routing.NodeWeighedAwayException;
import org.opensearch.cluster.routing.PreferenceBasedSearchNotAllowedException;
import org.opensearch.cluster.routing.UnsupportedWeightedRoutingStateException;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.pipeline.SearchPipelineProcessingException;
import org.opensearch.snapshots.SnapshotInUseDeletionException;
import org.opensearch.transport.TcpTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.Version.V_2_1_0;
import static org.opensearch.Version.V_2_4_0;
import static org.opensearch.Version.V_2_5_0;
import static org.opensearch.Version.V_2_6_0;
import static org.opensearch.Version.V_2_7_0;
import static org.opensearch.Version.V_3_0_0;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureFieldName;

/**
 * A base class for all opensearch exceptions.
 *
 * @opensearch.internal
 */
public class OpenSearchException extends BaseOpenSearchException implements Writeable {

    /**
     * Setting a higher base exception id to avoid conflicts.
     */
    private static final int CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID = 10000;

    private static final Map<Integer, CheckedFunction<StreamInput, ? extends OpenSearchException, IOException>> ID_TO_SUPPLIER;
    private static final Map<Class<? extends OpenSearchException>, OpenSearchExceptionHandle> CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE;

    private static final Pattern OS_METADATA = Pattern.compile("^opensearch\\.");

    /**
     * Construct a <code>OpenSearchException</code> with the specified cause exception.
     */
    public OpenSearchException(Throwable cause) {
        super(cause);
    }

    /**
     * Construct a <code>OpenSearchException</code> with the specified detail message.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg  the detail message
     * @param args the arguments for the message
     */
    public OpenSearchException(String msg, Object... args) {
        super(msg, args);
    }

    /**
     * Construct a <code>OpenSearchException</code> with the specified detail message
     * and nested exception.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg   the detail message
     * @param cause the nested exception
     * @param args  the arguments for the message
     */
    public OpenSearchException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public OpenSearchException(StreamInput in) throws IOException {
        super(in.readOptionalString(), in.readException());
        readStackTrace(this, in);
        headers.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
        metadata.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    /**
     * Returns the rest status code associated with this exception.
     */
    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        } else {
            return ExceptionsHelper.status(cause);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(this.getMessage());
        out.writeException(this.getCause());
        writeStackTraces(this, out, StreamOutput::writeException);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMapOfLists(metadata, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static OpenSearchException readException(StreamInput input, int id) throws IOException {
        CheckedFunction<StreamInput, ? extends OpenSearchException, IOException> opensearchException = ID_TO_SUPPLIER.get(id);
        if (opensearchException == null) {
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return opensearchException.apply(input);
    }

    /**
     * Returns <code>true</code> iff the given class is a registered for an exception to be read.
     */
    public static boolean isRegistered(Class<? extends Throwable> exception, Version version) {
        OpenSearchExceptionHandle openSearchExceptionHandle = CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE.get(exception);
        if (openSearchExceptionHandle != null) {
            return version.onOrAfter(openSearchExceptionHandle.versionAdded);
        }
        return false;
    }

    static Set<Class<? extends OpenSearchException>> getRegisteredKeys() { // for testing
        return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE.keySet();
    }

    /**
     * Returns the serialization id the given exception.
     */
    public static int getId(Class<? extends OpenSearchException> exception) {
        return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE.get(exception).id;
    }

    /**
     * Generate a {@link OpenSearchException} from a {@link XContentParser}. This does not
     * return the original exception type (ie NodeClosedException for example) but just wraps
     * the type, the reason and the cause of the exception. It also recursively parses the
     * tree structure of the cause, returning it as a tree structure of {@link OpenSearchException}
     * instances.
     */
    public static OpenSearchException fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return innerFromXContent(parser, false);
    }

    public static OpenSearchException innerFromXContent(XContentParser parser, boolean parseRootCauses) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String type = null, reason = null, stack = null;
        OpenSearchException cause = null;
        Map<String, List<String>> metadata = new HashMap<>();
        Map<String, List<String>> headers = new HashMap<>();
        List<OpenSearchException> rootCauses = new ArrayList<>();
        List<OpenSearchException> suppressed = new ArrayList<>();

        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String currentFieldName = parser.currentName();
            token = parser.nextToken();

            if (token.isValue()) {
                if (BaseExceptionsHelper.TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (BaseExceptionsHelper.REASON.equals(currentFieldName)) {
                    reason = parser.text();
                } else if (BaseExceptionsHelper.STACK_TRACE.equals(currentFieldName)) {
                    stack = parser.text();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    metadata.put(currentFieldName, Collections.singletonList(parser.text()));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BaseExceptionsHelper.CAUSED_BY.equals(currentFieldName)) {
                    cause = fromXContent(parser);
                } else if (BaseExceptionsHelper.HEADER.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            List<String> values = headers.getOrDefault(currentFieldName, new ArrayList<>());
                            if (token == XContentParser.Token.VALUE_STRING) {
                                values.add(parser.text());
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        values.add(parser.text());
                                    } else {
                                        parser.skipChildren();
                                    }
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                parser.skipChildren();
                            }
                            headers.put(currentFieldName, values);
                        }
                    }
                } else {
                    // Any additional metadata object added by the metadataToXContent method is ignored
                    // and skipped, so that the parser does not fail on unknown fields. The parser only
                    // support metadata key-pairs and metadata arrays of values.
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseRootCauses && ROOT_CAUSE.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rootCauses.add(fromXContent(parser));
                    }
                } else if (BaseExceptionsHelper.SUPPRESSED.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        suppressed.add(fromXContent(parser));
                    }
                } else {
                    // Parse the array and add each item to the corresponding list of metadata.
                    // Arrays of objects are not supported yet and just ignored and skipped.
                    List<String> values = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            values.add(parser.text());
                        } else {
                            parser.skipChildren();
                        }
                    }
                    if (values.size() > 0) {
                        if (metadata.containsKey(currentFieldName)) {
                            values.addAll(metadata.get(currentFieldName));
                        }
                        metadata.put(currentFieldName, values);
                    }
                }
            }
        }

        OpenSearchException e = new OpenSearchException(buildMessage(type, reason, stack), cause);
        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            // subclasses can print out additional metadata through the metadataToXContent method. Simple key-value pairs will be
            // parsed back and become part of this metadata set, while objects and arrays are not supported when parsing back.
            // Those key-value pairs become part of the metadata set and inherit the "opensearch." prefix as that is currently required
            // by addMetadata. The prefix will get stripped out when printing metadata out so it will be effectively invisible.
            // TODO move subclasses that print out simple metadata to using addMetadata directly and support also numbers and booleans.
            // TODO rename metadataToXContent and have only SearchPhaseExecutionException use it, which prints out complex objects
            e.addMetadata(BaseExceptionsHelper.OPENSEARCH_PREFIX_KEY + entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            e.addHeader(header.getKey(), header.getValue());
        }

        // Adds root causes as suppressed exception. This way they are not lost
        // after parsing and can be retrieved using getSuppressed() method.
        for (OpenSearchException rootCause : rootCauses) {
            e.addSuppressed(rootCause);
        }
        for (OpenSearchException s : suppressed) {
            e.addSuppressed(s);
        }
        return e;
    }

    /**
     * Parses the output of {@link #generateFailureXContent(XContentBuilder, Params, Exception, boolean)}
     */
    public static OpenSearchException failureFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureFieldName(parser, token, ERROR);

        token = parser.nextToken();
        if (token.isValue()) {
            return new OpenSearchException(buildMessage("exception", parser.text(), null));
        }

        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        // Root causes are parsed in the innerFromXContent() and are added as suppressed exceptions.
        return innerFromXContent(parser, true);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (metadata.containsKey(INDEX_METADATA_KEY)) {
            builder.append(getIndex());
            if (metadata.containsKey(SHARD_METADATA_KEY)) {
                builder.append('[').append(getShardId()).append(']');
            }
            builder.append(' ');
        }
        return builder.append(BaseExceptionsHelper.detailedMessage(this).trim()).toString();
    }

    /**
     * Deserializes stacktrace elements as well as suppressed exceptions from the given output stream and
     * adds it to the given exception.
     */
    public static <T extends Throwable> T readStackTrace(T throwable, StreamInput in) throws IOException {
        throwable.setStackTrace(in.readArray(i -> {
            final String declaringClasss = i.readString();
            final String fileName = i.readOptionalString();
            final String methodName = i.readString();
            final int lineNumber = i.readVInt();
            return new StackTraceElement(declaringClasss, methodName, fileName, lineNumber);
        }, StackTraceElement[]::new));

        int numSuppressed = in.readVInt();
        for (int i = 0; i < numSuppressed; i++) {
            throwable.addSuppressed(in.readException());
        }
        return throwable;
    }

    /**
     * Serializes the given exceptions stacktrace elements as well as it's suppressed exceptions to the given output stream.
     */
    public static <T extends Throwable> T writeStackTraces(T throwable, StreamOutput out, Writer<Throwable> exceptionWriter)
        throws IOException {
        out.writeArray((o, v) -> {
            o.writeString(v.getClassName());
            o.writeOptionalString(v.getFileName());
            o.writeString(v.getMethodName());
            o.writeVInt(v.getLineNumber());
        }, throwable.getStackTrace());
        out.writeArray(exceptionWriter, throwable.getSuppressed());
        return throwable;
    }

    /**
     * This is the list of Exceptions OpenSearch can throw over the wire or save into a corruption marker. Each value in the enum is a
     * single exception tying the Class to an id for use of the encode side and the id back to a constructor for use on the decode side. As
     * such its ok if the exceptions to change names so long as their constructor can still read the exception. Each exception is listed
     * in id order below. If you want to remove an exception leave a tombstone comment and mark the id as null in
     * ExceptionSerializationTests.testIds.ids.
     */
    private enum OpenSearchExceptionHandle {
        INDEX_SHARD_SNAPSHOT_FAILED_EXCEPTION(
            org.opensearch.index.snapshots.IndexShardSnapshotFailedException.class,
            org.opensearch.index.snapshots.IndexShardSnapshotFailedException::new,
            0,
            UNKNOWN_VERSION_ADDED
        ),
        DFS_PHASE_EXECUTION_EXCEPTION(
            org.opensearch.search.dfs.DfsPhaseExecutionException.class,
            org.opensearch.search.dfs.DfsPhaseExecutionException::new,
            1,
            UNKNOWN_VERSION_ADDED
        ),
        EXECUTION_CANCELLED_EXCEPTION(
            org.opensearch.common.util.CancellableThreads.ExecutionCancelledException.class,
            org.opensearch.common.util.CancellableThreads.ExecutionCancelledException::new,
            2,
            UNKNOWN_VERSION_ADDED
        ),
        CLUSTER_MANAGER_NOT_DISCOVERED_EXCEPTION(
            org.opensearch.discovery.ClusterManagerNotDiscoveredException.class,
            org.opensearch.discovery.ClusterManagerNotDiscoveredException::new,
            3,
            UNKNOWN_VERSION_ADDED
        ),
        OPENSEARCH_SECURITY_EXCEPTION(
            org.opensearch.OpenSearchSecurityException.class,
            org.opensearch.OpenSearchSecurityException::new,
            4,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RESTORE_EXCEPTION(
            org.opensearch.index.snapshots.IndexShardRestoreException.class,
            org.opensearch.index.snapshots.IndexShardRestoreException::new,
            5,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_CLOSED_EXCEPTION(
            org.opensearch.indices.IndexClosedException.class,
            org.opensearch.indices.IndexClosedException::new,
            6,
            UNKNOWN_VERSION_ADDED
        ),
        BIND_HTTP_EXCEPTION(
            org.opensearch.http.BindHttpException.class,
            org.opensearch.http.BindHttpException::new,
            7,
            UNKNOWN_VERSION_ADDED
        ),
        REDUCE_SEARCH_PHASE_EXCEPTION(
            org.opensearch.action.search.ReduceSearchPhaseException.class,
            org.opensearch.action.search.ReduceSearchPhaseException::new,
            8,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_CLOSED_EXCEPTION(
            org.opensearch.node.NodeClosedException.class,
            org.opensearch.node.NodeClosedException::new,
            9,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_FAILED_ENGINE_EXCEPTION(
            org.opensearch.index.engine.SnapshotFailedEngineException.class,
            org.opensearch.index.engine.SnapshotFailedEngineException::new,
            10,
            UNKNOWN_VERSION_ADDED
        ),
        SHARD_NOT_FOUND_EXCEPTION(
            org.opensearch.index.shard.ShardNotFoundException.class,
            org.opensearch.index.shard.ShardNotFoundException::new,
            11,
            UNKNOWN_VERSION_ADDED
        ),
        CONNECT_TRANSPORT_EXCEPTION(
            org.opensearch.transport.ConnectTransportException.class,
            org.opensearch.transport.ConnectTransportException::new,
            12,
            UNKNOWN_VERSION_ADDED
        ),
        NOT_SERIALIZABLE_TRANSPORT_EXCEPTION(
            org.opensearch.transport.NotSerializableTransportException.class,
            org.opensearch.transport.NotSerializableTransportException::new,
            13,
            UNKNOWN_VERSION_ADDED
        ),
        RESPONSE_HANDLER_FAILURE_TRANSPORT_EXCEPTION(
            org.opensearch.transport.ResponseHandlerFailureTransportException.class,
            org.opensearch.transport.ResponseHandlerFailureTransportException::new,
            14,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_CREATION_EXCEPTION(
            org.opensearch.indices.IndexCreationException.class,
            org.opensearch.indices.IndexCreationException::new,
            15,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_NOT_FOUND_EXCEPTION(
            org.opensearch.index.IndexNotFoundException.class,
            org.opensearch.index.IndexNotFoundException::new,
            16,
            UNKNOWN_VERSION_ADDED
        ),
        ILLEGAL_SHARD_ROUTING_STATE_EXCEPTION(
            org.opensearch.cluster.routing.IllegalShardRoutingStateException.class,
            org.opensearch.cluster.routing.IllegalShardRoutingStateException::new,
            17,
            UNKNOWN_VERSION_ADDED
        ),
        BROADCAST_SHARD_OPERATION_FAILED_EXCEPTION(
            org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException.class,
            org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException::new,
            18,
            UNKNOWN_VERSION_ADDED
        ),
        RESOURCE_NOT_FOUND_EXCEPTION(
            org.opensearch.ResourceNotFoundException.class,
            org.opensearch.ResourceNotFoundException::new,
            19,
            UNKNOWN_VERSION_ADDED
        ),
        ACTION_TRANSPORT_EXCEPTION(
            org.opensearch.transport.ActionTransportException.class,
            org.opensearch.transport.ActionTransportException::new,
            20,
            UNKNOWN_VERSION_ADDED
        ),
        OPENSEARCH_GENERATION_EXCEPTION(
            org.opensearch.OpenSearchGenerationException.class,
            org.opensearch.OpenSearchGenerationException::new,
            21,
            UNKNOWN_VERSION_ADDED
        ),
        // 22 was CreateFailedEngineException
        INDEX_SHARD_STARTED_EXCEPTION(
            org.opensearch.index.shard.IndexShardStartedException.class,
            org.opensearch.index.shard.IndexShardStartedException::new,
            23,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_CONTEXT_MISSING_EXCEPTION(
            org.opensearch.search.SearchContextMissingException.class,
            org.opensearch.search.SearchContextMissingException::new,
            24,
            UNKNOWN_VERSION_ADDED
        ),
        GENERAL_SCRIPT_EXCEPTION(
            org.opensearch.script.GeneralScriptException.class,
            org.opensearch.script.GeneralScriptException::new,
            25,
            UNKNOWN_VERSION_ADDED
        ),
        // 26 was BatchOperationException
        SNAPSHOT_CREATION_EXCEPTION(
            org.opensearch.snapshots.SnapshotCreationException.class,
            org.opensearch.snapshots.SnapshotCreationException::new,
            27,
            UNKNOWN_VERSION_ADDED
        ),
        // 28 was DeleteFailedEngineException, deprecated in 6.0, removed in 7.0
        DOCUMENT_MISSING_EXCEPTION(
            org.opensearch.index.engine.DocumentMissingException.class,
            org.opensearch.index.engine.DocumentMissingException::new,
            29,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_EXCEPTION(
            org.opensearch.snapshots.SnapshotException.class,
            org.opensearch.snapshots.SnapshotException::new,
            30,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_ALIAS_NAME_EXCEPTION(
            org.opensearch.indices.InvalidAliasNameException.class,
            org.opensearch.indices.InvalidAliasNameException::new,
            31,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_INDEX_NAME_EXCEPTION(
            org.opensearch.indices.InvalidIndexNameException.class,
            org.opensearch.indices.InvalidIndexNameException::new,
            32,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_PRIMARY_SHARD_NOT_ALLOCATED_EXCEPTION(
            org.opensearch.indices.IndexPrimaryShardNotAllocatedException.class,
            org.opensearch.indices.IndexPrimaryShardNotAllocatedException::new,
            33,
            UNKNOWN_VERSION_ADDED
        ),
        TRANSPORT_EXCEPTION(
            org.opensearch.transport.TransportException.class,
            org.opensearch.transport.TransportException::new,
            34,
            UNKNOWN_VERSION_ADDED
        ),
        OPENSEARCH_PARSE_EXCEPTION(
            org.opensearch.OpenSearchParseException.class,
            org.opensearch.OpenSearchParseException::new,
            35,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_EXCEPTION(
            org.opensearch.search.SearchException.class,
            org.opensearch.search.SearchException::new,
            36,
            UNKNOWN_VERSION_ADDED
        ),
        MAPPER_EXCEPTION(
            org.opensearch.index.mapper.MapperException.class,
            org.opensearch.index.mapper.MapperException::new,
            37,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_TYPE_NAME_EXCEPTION(
            org.opensearch.indices.InvalidTypeNameException.class,
            org.opensearch.indices.InvalidTypeNameException::new,
            38,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_RESTORE_EXCEPTION(
            org.opensearch.snapshots.SnapshotRestoreException.class,
            org.opensearch.snapshots.SnapshotRestoreException::new,
            39,
            UNKNOWN_VERSION_ADDED
        ),
        PARSING_EXCEPTION(
            org.opensearch.common.ParsingException.class,
            org.opensearch.common.ParsingException::new,
            40,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_CLOSED_EXCEPTION(
            org.opensearch.index.shard.IndexShardClosedException.class,
            org.opensearch.index.shard.IndexShardClosedException::new,
            41,
            UNKNOWN_VERSION_ADDED
        ),
        RECOVER_FILES_RECOVERY_EXCEPTION(
            org.opensearch.indices.recovery.RecoverFilesRecoveryException.class,
            org.opensearch.indices.recovery.RecoverFilesRecoveryException::new,
            42,
            UNKNOWN_VERSION_ADDED
        ),
        TRUNCATED_TRANSLOG_EXCEPTION(
            org.opensearch.index.translog.TruncatedTranslogException.class,
            org.opensearch.index.translog.TruncatedTranslogException::new,
            43,
            UNKNOWN_VERSION_ADDED
        ),
        RECOVERY_FAILED_EXCEPTION(
            org.opensearch.indices.recovery.RecoveryFailedException.class,
            org.opensearch.indices.recovery.RecoveryFailedException::new,
            44,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RELOCATED_EXCEPTION(
            org.opensearch.index.shard.IndexShardRelocatedException.class,
            org.opensearch.index.shard.IndexShardRelocatedException::new,
            45,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_SHOULD_NOT_CONNECT_EXCEPTION(
            org.opensearch.transport.NodeShouldNotConnectException.class,
            org.opensearch.transport.NodeShouldNotConnectException::new,
            46,
            UNKNOWN_VERSION_ADDED
        ),
        // 47 used to be for IndexTemplateAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        TRANSLOG_CORRUPTED_EXCEPTION(
            org.opensearch.index.translog.TranslogCorruptedException.class,
            org.opensearch.index.translog.TranslogCorruptedException::new,
            48,
            UNKNOWN_VERSION_ADDED
        ),
        CLUSTER_BLOCK_EXCEPTION(
            org.opensearch.cluster.block.ClusterBlockException.class,
            org.opensearch.cluster.block.ClusterBlockException::new,
            49,
            UNKNOWN_VERSION_ADDED
        ),
        FETCH_PHASE_EXECUTION_EXCEPTION(
            org.opensearch.search.fetch.FetchPhaseExecutionException.class,
            org.opensearch.search.fetch.FetchPhaseExecutionException::new,
            50,
            UNKNOWN_VERSION_ADDED
        ),
        // 51 used to be for IndexShardAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        VERSION_CONFLICT_ENGINE_EXCEPTION(
            org.opensearch.index.engine.VersionConflictEngineException.class,
            org.opensearch.index.engine.VersionConflictEngineException::new,
            52,
            UNKNOWN_VERSION_ADDED
        ),
        ENGINE_EXCEPTION(
            org.opensearch.index.engine.EngineException.class,
            org.opensearch.index.engine.EngineException::new,
            53,
            UNKNOWN_VERSION_ADDED
        ),
        // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
        NO_SUCH_NODE_EXCEPTION(
            org.opensearch.action.NoSuchNodeException.class,
            org.opensearch.action.NoSuchNodeException::new,
            55,
            UNKNOWN_VERSION_ADDED
        ),
        SETTINGS_EXCEPTION(
            org.opensearch.common.settings.SettingsException.class,
            org.opensearch.common.settings.SettingsException::new,
            56,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_TEMPLATE_MISSING_EXCEPTION(
            org.opensearch.indices.IndexTemplateMissingException.class,
            org.opensearch.indices.IndexTemplateMissingException::new,
            57,
            UNKNOWN_VERSION_ADDED
        ),
        SEND_REQUEST_TRANSPORT_EXCEPTION(
            org.opensearch.transport.SendRequestTransportException.class,
            org.opensearch.transport.SendRequestTransportException::new,
            58,
            UNKNOWN_VERSION_ADDED
        ),
        // 59 used to be OpenSearchRejectedExecutionException
        // 60 used to be for EarlyTerminationException
        // 61 used to be for RoutingValidationException
        NOT_SERIALIZABLE_EXCEPTION_WRAPPER(
            org.opensearch.common.io.stream.NotSerializableExceptionWrapper.class,
            org.opensearch.common.io.stream.NotSerializableExceptionWrapper::new,
            62,
            UNKNOWN_VERSION_ADDED
        ),
        ALIAS_FILTER_PARSING_EXCEPTION(
            org.opensearch.indices.AliasFilterParsingException.class,
            org.opensearch.indices.AliasFilterParsingException::new,
            63,
            UNKNOWN_VERSION_ADDED
        ),
        // 64 was DeleteByQueryFailedEngineException, which was removed in 5.0
        GATEWAY_EXCEPTION(
            org.opensearch.gateway.GatewayException.class,
            org.opensearch.gateway.GatewayException::new,
            65,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_NOT_RECOVERING_EXCEPTION(
            org.opensearch.index.shard.IndexShardNotRecoveringException.class,
            org.opensearch.index.shard.IndexShardNotRecoveringException::new,
            66,
            UNKNOWN_VERSION_ADDED
        ),
        HTTP_EXCEPTION(org.opensearch.http.HttpException.class, org.opensearch.http.HttpException::new, 67, UNKNOWN_VERSION_ADDED),
        OPENSEARCH_EXCEPTION(OpenSearchException.class, OpenSearchException::new, 68, UNKNOWN_VERSION_ADDED),
        SNAPSHOT_MISSING_EXCEPTION(
            org.opensearch.snapshots.SnapshotMissingException.class,
            org.opensearch.snapshots.SnapshotMissingException::new,
            69,
            UNKNOWN_VERSION_ADDED
        ),
        PRIMARY_MISSING_ACTION_EXCEPTION(
            org.opensearch.action.PrimaryMissingActionException.class,
            org.opensearch.action.PrimaryMissingActionException::new,
            70,
            UNKNOWN_VERSION_ADDED
        ),
        FAILED_NODE_EXCEPTION(
            org.opensearch.action.FailedNodeException.class,
            org.opensearch.action.FailedNodeException::new,
            71,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_PARSE_EXCEPTION(
            org.opensearch.search.SearchParseException.class,
            org.opensearch.search.SearchParseException::new,
            72,
            UNKNOWN_VERSION_ADDED
        ),
        CONCURRENT_SNAPSHOT_EXECUTION_EXCEPTION(
            org.opensearch.snapshots.ConcurrentSnapshotExecutionException.class,
            org.opensearch.snapshots.ConcurrentSnapshotExecutionException::new,
            73,
            UNKNOWN_VERSION_ADDED
        ),
        BLOB_STORE_EXCEPTION(
            org.opensearch.common.blobstore.BlobStoreException.class,
            org.opensearch.common.blobstore.BlobStoreException::new,
            74,
            UNKNOWN_VERSION_ADDED
        ),
        INCOMPATIBLE_CLUSTER_STATE_VERSION_EXCEPTION(
            org.opensearch.cluster.IncompatibleClusterStateVersionException.class,
            org.opensearch.cluster.IncompatibleClusterStateVersionException::new,
            75,
            UNKNOWN_VERSION_ADDED
        ),
        RECOVERY_ENGINE_EXCEPTION(
            org.opensearch.index.engine.RecoveryEngineException.class,
            org.opensearch.index.engine.RecoveryEngineException::new,
            76,
            UNKNOWN_VERSION_ADDED
        ),
        UNCATEGORIZED_EXECUTION_EXCEPTION(
            org.opensearch.common.util.concurrent.UncategorizedExecutionException.class,
            org.opensearch.common.util.concurrent.UncategorizedExecutionException::new,
            77,
            UNKNOWN_VERSION_ADDED
        ),
        TIMESTAMP_PARSING_EXCEPTION(
            org.opensearch.action.TimestampParsingException.class,
            org.opensearch.action.TimestampParsingException::new,
            78,
            UNKNOWN_VERSION_ADDED
        ),
        ROUTING_MISSING_EXCEPTION(
            org.opensearch.action.RoutingMissingException.class,
            org.opensearch.action.RoutingMissingException::new,
            79,
            UNKNOWN_VERSION_ADDED
        ),
        // 80 was IndexFailedEngineException, deprecated in 6.0, removed in 7.0
        INDEX_SHARD_RESTORE_FAILED_EXCEPTION(
            org.opensearch.index.snapshots.IndexShardRestoreFailedException.class,
            org.opensearch.index.snapshots.IndexShardRestoreFailedException::new,
            81,
            UNKNOWN_VERSION_ADDED
        ),
        REPOSITORY_EXCEPTION(
            org.opensearch.repositories.RepositoryException.class,
            org.opensearch.repositories.RepositoryException::new,
            82,
            UNKNOWN_VERSION_ADDED
        ),
        RECEIVE_TIMEOUT_TRANSPORT_EXCEPTION(
            org.opensearch.transport.ReceiveTimeoutTransportException.class,
            org.opensearch.transport.ReceiveTimeoutTransportException::new,
            83,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_DISCONNECTED_EXCEPTION(
            org.opensearch.transport.NodeDisconnectedException.class,
            org.opensearch.transport.NodeDisconnectedException::new,
            84,
            UNKNOWN_VERSION_ADDED
        ),
        // 85 used to be for AlreadyExpiredException
        AGGREGATION_EXECUTION_EXCEPTION(
            org.opensearch.search.aggregations.AggregationExecutionException.class,
            org.opensearch.search.aggregations.AggregationExecutionException::new,
            86,
            UNKNOWN_VERSION_ADDED
        ),
        // 87 used to be for MergeMappingException
        INVALID_INDEX_TEMPLATE_EXCEPTION(
            org.opensearch.indices.InvalidIndexTemplateException.class,
            org.opensearch.indices.InvalidIndexTemplateException::new,
            88,
            UNKNOWN_VERSION_ADDED
        ),
        REFRESH_FAILED_ENGINE_EXCEPTION(
            org.opensearch.index.engine.RefreshFailedEngineException.class,
            org.opensearch.index.engine.RefreshFailedEngineException::new,
            90,
            UNKNOWN_VERSION_ADDED
        ),
        AGGREGATION_INITIALIZATION_EXCEPTION(
            org.opensearch.search.aggregations.AggregationInitializationException.class,
            org.opensearch.search.aggregations.AggregationInitializationException::new,
            91,
            UNKNOWN_VERSION_ADDED
        ),
        DELAY_RECOVERY_EXCEPTION(
            org.opensearch.indices.recovery.DelayRecoveryException.class,
            org.opensearch.indices.recovery.DelayRecoveryException::new,
            92,
            UNKNOWN_VERSION_ADDED
        ),
        // 93 used to be for IndexWarmerMissingException
        NO_NODE_AVAILABLE_EXCEPTION(
            org.opensearch.client.transport.NoNodeAvailableException.class,
            org.opensearch.client.transport.NoNodeAvailableException::new,
            94,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_SNAPSHOT_NAME_EXCEPTION(
            org.opensearch.snapshots.InvalidSnapshotNameException.class,
            org.opensearch.snapshots.InvalidSnapshotNameException::new,
            96,
            UNKNOWN_VERSION_ADDED
        ),
        ILLEGAL_INDEX_SHARD_STATE_EXCEPTION(
            org.opensearch.index.shard.IllegalIndexShardStateException.class,
            org.opensearch.index.shard.IllegalIndexShardStateException::new,
            97,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_SNAPSHOT_EXCEPTION(
            org.opensearch.index.snapshots.IndexShardSnapshotException.class,
            org.opensearch.index.snapshots.IndexShardSnapshotException::new,
            98,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_NOT_STARTED_EXCEPTION(
            org.opensearch.index.shard.IndexShardNotStartedException.class,
            org.opensearch.index.shard.IndexShardNotStartedException::new,
            99,
            UNKNOWN_VERSION_ADDED
        ),
        SEARCH_PHASE_EXECUTION_EXCEPTION(
            org.opensearch.action.search.SearchPhaseExecutionException.class,
            org.opensearch.action.search.SearchPhaseExecutionException::new,
            100,
            UNKNOWN_VERSION_ADDED
        ),
        ACTION_NOT_FOUND_TRANSPORT_EXCEPTION(
            org.opensearch.transport.ActionNotFoundTransportException.class,
            org.opensearch.transport.ActionNotFoundTransportException::new,
            101,
            UNKNOWN_VERSION_ADDED
        ),
        TRANSPORT_SERIALIZATION_EXCEPTION(
            org.opensearch.transport.TransportSerializationException.class,
            org.opensearch.transport.TransportSerializationException::new,
            102,
            UNKNOWN_VERSION_ADDED
        ),
        REMOTE_TRANSPORT_EXCEPTION(
            org.opensearch.transport.RemoteTransportException.class,
            org.opensearch.transport.RemoteTransportException::new,
            103,
            UNKNOWN_VERSION_ADDED
        ),
        ENGINE_CREATION_FAILURE_EXCEPTION(
            org.opensearch.index.engine.EngineCreationFailureException.class,
            org.opensearch.index.engine.EngineCreationFailureException::new,
            104,
            UNKNOWN_VERSION_ADDED
        ),
        ROUTING_EXCEPTION(
            org.opensearch.cluster.routing.RoutingException.class,
            org.opensearch.cluster.routing.RoutingException::new,
            105,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RECOVERY_EXCEPTION(
            org.opensearch.index.shard.IndexShardRecoveryException.class,
            org.opensearch.index.shard.IndexShardRecoveryException::new,
            106,
            UNKNOWN_VERSION_ADDED
        ),
        REPOSITORY_MISSING_EXCEPTION(
            org.opensearch.repositories.RepositoryMissingException.class,
            org.opensearch.repositories.RepositoryMissingException::new,
            107,
            UNKNOWN_VERSION_ADDED
        ),
        DOCUMENT_SOURCE_MISSING_EXCEPTION(
            org.opensearch.index.engine.DocumentSourceMissingException.class,
            org.opensearch.index.engine.DocumentSourceMissingException::new,
            109,
            UNKNOWN_VERSION_ADDED
        ),
        // 110 used to be FlushNotAllowedEngineException
        NO_CLASS_SETTINGS_EXCEPTION(
            org.opensearch.common.settings.NoClassSettingsException.class,
            org.opensearch.common.settings.NoClassSettingsException::new,
            111,
            UNKNOWN_VERSION_ADDED
        ),
        BIND_TRANSPORT_EXCEPTION(
            org.opensearch.transport.BindTransportException.class,
            org.opensearch.transport.BindTransportException::new,
            112,
            UNKNOWN_VERSION_ADDED
        ),
        ALIASES_NOT_FOUND_EXCEPTION(
            org.opensearch.rest.action.admin.indices.AliasesNotFoundException.class,
            org.opensearch.rest.action.admin.indices.AliasesNotFoundException::new,
            113,
            UNKNOWN_VERSION_ADDED
        ),
        INDEX_SHARD_RECOVERING_EXCEPTION(
            org.opensearch.index.shard.IndexShardRecoveringException.class,
            org.opensearch.index.shard.IndexShardRecoveringException::new,
            114,
            UNKNOWN_VERSION_ADDED
        ),
        TRANSLOG_EXCEPTION(
            org.opensearch.index.translog.TranslogException.class,
            org.opensearch.index.translog.TranslogException::new,
            115,
            UNKNOWN_VERSION_ADDED
        ),
        PROCESS_CLUSTER_EVENT_TIMEOUT_EXCEPTION(
            org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException.class,
            org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException::new,
            116,
            UNKNOWN_VERSION_ADDED
        ),
        RETRY_ON_PRIMARY_EXCEPTION(
            ReplicationOperation.RetryOnPrimaryException.class,
            ReplicationOperation.RetryOnPrimaryException::new,
            117,
            UNKNOWN_VERSION_ADDED
        ),
        OPENSEARCH_TIMEOUT_EXCEPTION(
            org.opensearch.OpenSearchTimeoutException.class,
            org.opensearch.OpenSearchTimeoutException::new,
            118,
            UNKNOWN_VERSION_ADDED
        ),
        QUERY_PHASE_EXECUTION_EXCEPTION(
            org.opensearch.search.query.QueryPhaseExecutionException.class,
            org.opensearch.search.query.QueryPhaseExecutionException::new,
            119,
            UNKNOWN_VERSION_ADDED
        ),
        REPOSITORY_VERIFICATION_EXCEPTION(
            org.opensearch.repositories.RepositoryVerificationException.class,
            org.opensearch.repositories.RepositoryVerificationException::new,
            120,
            UNKNOWN_VERSION_ADDED
        ),
        INVALID_AGGREGATION_PATH_EXCEPTION(
            org.opensearch.search.aggregations.InvalidAggregationPathException.class,
            org.opensearch.search.aggregations.InvalidAggregationPathException::new,
            121,
            UNKNOWN_VERSION_ADDED
        ),
        // 123 used to be IndexAlreadyExistsException and was renamed
        RESOURCE_ALREADY_EXISTS_EXCEPTION(
            ResourceAlreadyExistsException.class,
            ResourceAlreadyExistsException::new,
            123,
            UNKNOWN_VERSION_ADDED
        ),
        // 124 used to be Script.ScriptParseException
        HTTP_REQUEST_ON_TRANSPORT_EXCEPTION(
            TcpTransport.HttpRequestOnTransportException.class,
            TcpTransport.HttpRequestOnTransportException::new,
            125,
            UNKNOWN_VERSION_ADDED
        ),
        MAPPER_PARSING_EXCEPTION(
            org.opensearch.index.mapper.MapperParsingException.class,
            org.opensearch.index.mapper.MapperParsingException::new,
            126,
            UNKNOWN_VERSION_ADDED
        ),
        // 127 used to be org.opensearch.search.SearchContextException
        SEARCH_SOURCE_BUILDER_EXCEPTION(
            org.opensearch.search.builder.SearchSourceBuilderException.class,
            org.opensearch.search.builder.SearchSourceBuilderException::new,
            128,
            UNKNOWN_VERSION_ADDED
        ),
        // 129 was EngineClosedException
        NO_SHARD_AVAILABLE_ACTION_EXCEPTION(
            org.opensearch.action.NoShardAvailableActionException.class,
            org.opensearch.action.NoShardAvailableActionException::new,
            130,
            UNKNOWN_VERSION_ADDED
        ),
        UNAVAILABLE_SHARDS_EXCEPTION(
            org.opensearch.action.UnavailableShardsException.class,
            org.opensearch.action.UnavailableShardsException::new,
            131,
            UNKNOWN_VERSION_ADDED
        ),
        FLUSH_FAILED_ENGINE_EXCEPTION(
            org.opensearch.index.engine.FlushFailedEngineException.class,
            org.opensearch.index.engine.FlushFailedEngineException::new,
            132,
            UNKNOWN_VERSION_ADDED
        ),
        CIRCUIT_BREAKING_EXCEPTION(
            org.opensearch.common.breaker.CircuitBreakingException.class,
            org.opensearch.common.breaker.CircuitBreakingException::new,
            133,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_NOT_CONNECTED_EXCEPTION(
            org.opensearch.transport.NodeNotConnectedException.class,
            org.opensearch.transport.NodeNotConnectedException::new,
            134,
            UNKNOWN_VERSION_ADDED
        ),
        STRICT_DYNAMIC_MAPPING_EXCEPTION(
            org.opensearch.index.mapper.StrictDynamicMappingException.class,
            org.opensearch.index.mapper.StrictDynamicMappingException::new,
            135,
            UNKNOWN_VERSION_ADDED
        ),
        RETRY_ON_REPLICA_EXCEPTION(
            org.opensearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
            org.opensearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new,
            136,
            UNKNOWN_VERSION_ADDED
        ),
        TYPE_MISSING_EXCEPTION(
            org.opensearch.indices.TypeMissingException.class,
            org.opensearch.indices.TypeMissingException::new,
            137,
            UNKNOWN_VERSION_ADDED
        ),
        FAILED_TO_COMMIT_CLUSTER_STATE_EXCEPTION(
            org.opensearch.cluster.coordination.FailedToCommitClusterStateException.class,
            org.opensearch.cluster.coordination.FailedToCommitClusterStateException::new,
            140,
            UNKNOWN_VERSION_ADDED
        ),
        QUERY_SHARD_EXCEPTION(
            org.opensearch.index.query.QueryShardException.class,
            org.opensearch.index.query.QueryShardException::new,
            141,
            UNKNOWN_VERSION_ADDED
        ),
        NO_LONGER_PRIMARY_SHARD_EXCEPTION(
            ShardStateAction.NoLongerPrimaryShardException.class,
            ShardStateAction.NoLongerPrimaryShardException::new,
            142,
            UNKNOWN_VERSION_ADDED
        ),
        SCRIPT_EXCEPTION(
            org.opensearch.script.ScriptException.class,
            org.opensearch.script.ScriptException::new,
            143,
            UNKNOWN_VERSION_ADDED
        ),
        NOT_CLUSTER_MANAGER_EXCEPTION(
            org.opensearch.cluster.NotClusterManagerException.class,
            org.opensearch.cluster.NotClusterManagerException::new,
            144,
            UNKNOWN_VERSION_ADDED
        ),
        STATUS_EXCEPTION(
            org.opensearch.OpenSearchStatusException.class,
            org.opensearch.OpenSearchStatusException::new,
            145,
            UNKNOWN_VERSION_ADDED
        ),
        TASK_CANCELLED_EXCEPTION(
            org.opensearch.tasks.TaskCancelledException.class,
            org.opensearch.tasks.TaskCancelledException::new,
            146,
            UNKNOWN_VERSION_ADDED
        ),
        SHARD_LOCK_OBTAIN_FAILED_EXCEPTION(
            org.opensearch.env.ShardLockObtainFailedException.class,
            org.opensearch.env.ShardLockObtainFailedException::new,
            147,
            UNKNOWN_VERSION_ADDED
        ),
        // 148 was UnknownNamedObjectException
        TOO_MANY_BUCKETS_EXCEPTION(
            MultiBucketConsumerService.TooManyBucketsException.class,
            MultiBucketConsumerService.TooManyBucketsException::new,
            149,
            UNKNOWN_VERSION_ADDED
        ),
        COORDINATION_STATE_REJECTED_EXCEPTION(
            org.opensearch.cluster.coordination.CoordinationStateRejectedException.class,
            org.opensearch.cluster.coordination.CoordinationStateRejectedException::new,
            150,
            UNKNOWN_VERSION_ADDED
        ),
        SNAPSHOT_IN_PROGRESS_EXCEPTION(
            org.opensearch.snapshots.SnapshotInProgressException.class,
            org.opensearch.snapshots.SnapshotInProgressException::new,
            151,
            UNKNOWN_VERSION_ADDED
        ),
        NO_SUCH_REMOTE_CLUSTER_EXCEPTION(
            org.opensearch.transport.NoSuchRemoteClusterException.class,
            org.opensearch.transport.NoSuchRemoteClusterException::new,
            152,
            UNKNOWN_VERSION_ADDED
        ),
        RETENTION_LEASE_ALREADY_EXISTS_EXCEPTION(
            org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException.class,
            org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException::new,
            153,
            UNKNOWN_VERSION_ADDED
        ),
        RETENTION_LEASE_NOT_FOUND_EXCEPTION(
            org.opensearch.index.seqno.RetentionLeaseNotFoundException.class,
            org.opensearch.index.seqno.RetentionLeaseNotFoundException::new,
            154,
            UNKNOWN_VERSION_ADDED
        ),
        SHARD_NOT_IN_PRIMARY_MODE_EXCEPTION(
            org.opensearch.index.shard.ShardNotInPrimaryModeException.class,
            org.opensearch.index.shard.ShardNotInPrimaryModeException::new,
            155,
            UNKNOWN_VERSION_ADDED
        ),
        RETENTION_LEASE_INVALID_RETAINING_SEQUENCE_NUMBER_EXCEPTION(
            org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException.class,
            org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException::new,
            156,
            UNKNOWN_VERSION_ADDED
        ),
        INGEST_PROCESSOR_EXCEPTION(
            org.opensearch.ingest.IngestProcessorException.class,
            org.opensearch.ingest.IngestProcessorException::new,
            157,
            UNKNOWN_VERSION_ADDED
        ),
        PEER_RECOVERY_NOT_FOUND_EXCEPTION(
            org.opensearch.indices.recovery.PeerRecoveryNotFound.class,
            org.opensearch.indices.recovery.PeerRecoveryNotFound::new,
            158,
            UNKNOWN_VERSION_ADDED
        ),
        NODE_HEALTH_CHECK_FAILURE_EXCEPTION(
            org.opensearch.cluster.coordination.NodeHealthCheckFailureException.class,
            org.opensearch.cluster.coordination.NodeHealthCheckFailureException::new,
            159,
            UNKNOWN_VERSION_ADDED
        ),
        NO_SEED_NODE_LEFT_EXCEPTION(
            org.opensearch.transport.NoSeedNodeLeftException.class,
            org.opensearch.transport.NoSeedNodeLeftException::new,
            160,
            UNKNOWN_VERSION_ADDED
        ),
        REPLICATION_FAILED_EXCEPTION(
            org.opensearch.indices.replication.common.ReplicationFailedException.class,
            org.opensearch.indices.replication.common.ReplicationFailedException::new,
            161,
            V_2_1_0
        ),
        PRIMARY_SHARD_CLOSED_EXCEPTION(
            org.opensearch.index.shard.PrimaryShardClosedException.class,
            org.opensearch.index.shard.PrimaryShardClosedException::new,
            162,
            V_3_0_0
        ),
        DECOMMISSIONING_FAILED_EXCEPTION(
            org.opensearch.cluster.decommission.DecommissioningFailedException.class,
            org.opensearch.cluster.decommission.DecommissioningFailedException::new,
            163,
            V_2_4_0
        ),
        NODE_DECOMMISSIONED_EXCEPTION(
            org.opensearch.cluster.decommission.NodeDecommissionedException.class,
            org.opensearch.cluster.decommission.NodeDecommissionedException::new,
            164,
            V_3_0_0
        ),
        CLUSTER_MANAGER_TASK_THROTTLED_EXCEPTION(
            ClusterManagerThrottlingException.class,
            ClusterManagerThrottlingException::new,
            165,
            Version.V_2_5_0
        ),
        SNAPSHOT_IN_USE_DELETION_EXCEPTION(
            SnapshotInUseDeletionException.class,
            SnapshotInUseDeletionException::new,
            166,
            UNKNOWN_VERSION_ADDED
        ),
        UNSUPPORTED_WEIGHTED_ROUTING_STATE_EXCEPTION(
            UnsupportedWeightedRoutingStateException.class,
            UnsupportedWeightedRoutingStateException::new,
            167,
            V_2_5_0
        ),
        PREFERENCE_BASED_SEARCH_NOT_ALLOWED_EXCEPTION(
            PreferenceBasedSearchNotAllowedException.class,
            PreferenceBasedSearchNotAllowedException::new,
            168,
            V_2_6_0
        ),
        NODE_WEIGHED_AWAY_EXCEPTION(NodeWeighedAwayException.class, NodeWeighedAwayException::new, 169, V_2_6_0),
        SEARCH_PIPELINE_PROCESSING_EXCEPTION(SearchPipelineProcessingException.class, SearchPipelineProcessingException::new, 170, V_2_7_0),
        INDEX_CREATE_BLOCK_EXCEPTION(
            org.opensearch.cluster.block.IndexCreateBlockException.class,
            org.opensearch.cluster.block.IndexCreateBlockException::new,
            CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID + 1,
            V_3_0_0
        );

        final Class<? extends OpenSearchException> exceptionClass;
        final CheckedFunction<StreamInput, ? extends OpenSearchException, IOException> constructor;
        final int id;
        final Version versionAdded;

        <E extends OpenSearchException> OpenSearchExceptionHandle(
            Class<E> exceptionClass,
            CheckedFunction<StreamInput, E, IOException> constructor,
            int id,
            Version versionAdded
        ) {
            // We need the exceptionClass because you can't dig it out of the constructor reliably.
            this.exceptionClass = exceptionClass;
            this.constructor = constructor;
            this.versionAdded = versionAdded;
            this.id = id;
        }
    }

    /**
     * Returns an array of all registered handle IDs. These are the IDs for every registered
     * exception.
     *
     * @return an array of all registered handle IDs
     */
    static int[] ids() {
        return Arrays.stream(OpenSearchExceptionHandle.values()).mapToInt(h -> h.id).toArray();
    }

    /**
     * Returns an array of all registered pairs of handle IDs and exception classes. These pairs are
     * provided for every registered exception.
     *
     * @return an array of all registered pairs of handle IDs and exception classes
     */
    static Tuple<Integer, Class<? extends OpenSearchException>>[] classes() {
        @SuppressWarnings("unchecked")
        final Tuple<Integer, Class<? extends OpenSearchException>>[] ts = Arrays.stream(OpenSearchExceptionHandle.values())
            .map(h -> Tuple.tuple(h.id, h.exceptionClass))
            .toArray(Tuple[]::new);
        return ts;
    }

    static {
        ID_TO_SUPPLIER = unmodifiableMap(
            Arrays.stream(OpenSearchExceptionHandle.values()).collect(Collectors.toMap(e -> e.id, e -> e.constructor))
        );
        CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE = unmodifiableMap(
            Arrays.stream(OpenSearchExceptionHandle.values()).collect(Collectors.toMap(e -> e.exceptionClass, e -> e))
        );
    }

    public Index getIndex() {
        List<String> index = getMetadata(INDEX_METADATA_KEY);
        if (index != null && index.isEmpty() == false) {
            List<String> index_uuid = getMetadata(INDEX_METADATA_KEY_UUID);
            return new Index(index.get(0), index_uuid.get(0));
        }

        return null;
    }

    public ShardId getShardId() {
        List<String> shard = getMetadata(SHARD_METADATA_KEY);
        if (shard != null && shard.isEmpty() == false) {
            return new ShardId(getIndex(), Integer.parseInt(shard.get(0)));
        }
        return null;
    }

    public void setIndex(Index index) {
        if (index != null) {
            addMetadata(INDEX_METADATA_KEY, index.getName());
            addMetadata(INDEX_METADATA_KEY_UUID, index.getUUID());
        }
    }

    public void setIndex(String index) {
        if (index != null) {
            setIndex(new Index(index, INDEX_UUID_NA_VALUE));
        }
    }

    public void setShard(ShardId shardId) {
        if (shardId != null) {
            setIndex(shardId.getIndex());
            addMetadata(SHARD_METADATA_KEY, Integer.toString(shardId.id()));
        }
    }

}
