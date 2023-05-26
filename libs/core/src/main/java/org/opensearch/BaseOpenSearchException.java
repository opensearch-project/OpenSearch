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

import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.index.snapshots.IndexShardSnapshotFailedException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.BaseOpenSearchException.OpenSearchExceptionHandleRegistry.registerExceptionHandle;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureFieldName;

import static java.util.Collections.singletonMap;

/**
 * A core library base class for all opensearch exceptions.
 *
 * @opensearch.internal
 */
@SuppressWarnings("rawtypes")
public abstract class BaseOpenSearchException extends RuntimeException implements Writeable, ToXContentFragment {

    protected static final String ERROR = "error";
    protected static final String ROOT_CAUSE = "root_cause";
    protected static final String RESOURCE_METADATA_TYPE_KEY = "opensearch.resource.type";
    protected static final String RESOURCE_METADATA_ID_KEY = "opensearch.resource.id";
    protected static final Version UNKNOWN_VERSION_ADDED = Version.fromId(0);
    protected static final String INDEX_METADATA_KEY = "opensearch.index";
    protected static final String SHARD_METADATA_KEY = "opensearch.shard";
    protected static final String INDEX_METADATA_KEY_UUID = "opensearch.index_uuid";
    protected final Map<String, List<String>> metadata = new HashMap<>();
    protected final Map<String, List<String>> headers = new HashMap<>();

    static {
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                IndexShardSnapshotFailedException.class,
                IndexShardSnapshotFailedException::new,
                0,
                UNKNOWN_VERSION_ADDED
            )
        );
    }

    /**
     * Construct a <code>BaseOpenSearchException</code> with the specified cause exception.
     */
    public BaseOpenSearchException(Throwable cause) {
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
    public BaseOpenSearchException(String msg, Object... args) {
        super(LoggerMessageFormat.format(msg, args));
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
    public BaseOpenSearchException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }

    public BaseOpenSearchException(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readException());
        readStackTrace(this, in);
        headers.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
        metadata.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalString(this.getMessage());
        out.writeException(this.getCause());
        writeStackTraces(this, out, StreamOutput::writeException);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMapOfLists(metadata, StreamOutput::writeString, StreamOutput::writeString);
    }

    /**
     * Render any exception as a xcontent, encapsulated within a field or object named "error". The level of details that are rendered
     * depends on the value of the "detailed" parameter: when it's false only a simple message based on the type and message of the
     * exception is rendered. When it's true all detail are provided including guesses root causes, cause and potentially stack
     * trace.
     *
     * This method is usually used when the {@link Exception} is rendered as a full XContent object, and its output can be parsed
     * by the {@code #OpenSearchException.failureFromXContent(XContentParser)} method.
     */
    public static void generateFailureXContent(XContentBuilder builder, ToXContent.Params params, @Nullable Exception e, boolean detailed)
        throws IOException {
        // No exception to render as an error
        if (e == null) {
            builder.field(ERROR, "unknown");
            return;
        }

        // Render the exception with a simple message
        if (detailed == false) {
            Throwable t = e;
            for (int counter = 0; counter < 10 && t != null; counter++) {
                if (t instanceof BaseOpenSearchException) {
                    break;
                }
                t = t.getCause();
            }
            builder.field(ERROR, BaseExceptionsHelper.summaryMessage(t != null ? t : e));
            return;
        }

        // Render the exception with all details
        final BaseOpenSearchException[] rootCauses = BaseOpenSearchException.guessRootCauses(e);
        builder.startObject(ERROR);
        {
            builder.startArray(ROOT_CAUSE);
            for (BaseOpenSearchException rootCause : rootCauses) {
                builder.startObject();
                rootCause.toXContent(
                    builder,
                    new ToXContent.DelegatingMapParams(singletonMap(BaseExceptionsHelper.REST_EXCEPTION_SKIP_CAUSE, "true"), params)
                );
                builder.endObject();
            }
            builder.endArray();
        }
        BaseExceptionsHelper.generateThrowableXContent(builder, params, e);
        builder.endObject();
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions.
     * If the given exception is not an instance of {@link BaseOpenSearchException} an empty array
     * is returned.
     */
    public static BaseOpenSearchException[] guessRootCauses(Throwable t) {
        Throwable ex = BaseExceptionsHelper.unwrapCause(t);
        if (ex instanceof BaseOpenSearchException) {
            // OpenSearchException knows how to guess its own root cause
            return ((BaseOpenSearchException) ex).guessRootCauses();
        }
        if (ex instanceof XContentParseException) {
            /*
             * We'd like to unwrap parsing exceptions to the inner-most
             * parsing exception because that is generally the most interesting
             * exception to return to the user. If that exception is caused by
             * an OpenSearchException we'd like to keep unwrapping because
             * OpenSearchException instances tend to contain useful information
             * for the user.
             */
            Throwable cause = ex.getCause();
            if (cause != null) {
                if (cause instanceof XContentParseException || cause instanceof BaseOpenSearchException) {
                    return BaseOpenSearchException.guessRootCauses(ex.getCause());
                }
            }
        }
        return new BaseOpenSearchException[] { new BaseOpenSearchException(ex.getMessage(), ex) {
            @Override
            protected String getExceptionName() {
                return BaseExceptionsHelper.getExceptionName(getCause());
            }
        } };
    }

    static String buildMessage(String type, String reason, String stack) {
        StringBuilder message = new StringBuilder("OpenSearch exception [");
        message.append(BaseExceptionsHelper.TYPE).append('=').append(type).append(", ");
        message.append(BaseExceptionsHelper.REASON).append('=').append(reason);
        if (stack != null) {
            message.append(", ").append(BaseExceptionsHelper.STACK_TRACE).append('=').append(stack);
        }
        message.append(']');
        return message.toString();
    }

    /**
     * Adds a new piece of metadata with the given key.
     * If the provided key is already present, the corresponding metadata will be replaced
     */
    public void addMetadata(String key, String... values) {
        addMetadata(key, Arrays.asList(values));
    }

    /**
     * Adds a new piece of metadata with the given key.
     * If the provided key is already present, the corresponding metadata will be replaced
     */
    public void addMetadata(String key, List<String> values) {
        // we need to enforce this otherwise bw comp doesn't work properly, as "opensearch."
        // was the previous criteria to split headers in two sets
        if (key.startsWith(BaseExceptionsHelper.OPENSEARCH_PREFIX_KEY) == false) {
            throw new IllegalArgumentException("exception metadata must start with [opensearch.], found [" + key + "] instead");
        }
        this.metadata.put(key, values);
    }

    /**
     * Returns a set of all metadata keys on this exception
     */
    public Set<String> getMetadataKeys() {
        return metadata.keySet();
    }

    /**
     * Returns the list of metadata values for the given key or {@code null} if no metadata for the
     * given key exists.
     */
    public List<String> getMetadata(String key) {
        return metadata.get(key);
    }

    protected Map<String, List<String>> getMetadata() {
        return metadata;
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, List<String> value) {
        // we need to enforce this otherwise bw comp doesn't work properly, as "opensearch."
        // was the previous criteria to split headers in two sets
        if (key.startsWith(BaseExceptionsHelper.OPENSEARCH_PREFIX_KEY)) {
            throw new IllegalArgumentException("exception headers must not start with [opensearch.], found [" + key + "] instead");
        }
        this.headers.put(key, value);
    }

    /**
     * Adds a new header with the given key.
     * This method will replace existing header if a header with the same key already exists
     */
    public void addHeader(String key, String... value) {
        addHeader(key, Arrays.asList(value));
    }

    /**
     * Returns a set of all header keys on this exception
     */
    public Set<String> getHeaderKeys() {
        return headers.keySet();
    }

    /**
     * Returns the list of header values for the given key or {@code null} if no header for the
     * given key exists.
     */
    public List<String> getHeader(String key) {
        return headers.get(key);
    }

    protected Map<String, List<String>> getHeaders() {
        return headers;
    }

    /**
     * Unwraps the actual cause from the exception for cases when the exception is a
     * {@link OpenSearchWrapperException}.
     *
     * @see BaseExceptionsHelper#unwrapCause(Throwable)
     */
    public Throwable unwrapCause() {
        return BaseExceptionsHelper.unwrapCause(this);
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(toString()).append("; ");
            if (getCause() instanceof BaseOpenSearchException) {
                sb.append(((BaseOpenSearchException) getCause()).getDetailedMessage());
            } else {
                sb.append(getCause());
            }
            return sb.toString();
        } else {
            return toString();
        }
    }

    /**
     * Retrieve the innermost cause of this exception, if none, returns the current exception.
     */
    public Throwable getRootCause() {
        Throwable rootCause = this;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

    /**
     * Renders additional per exception information into the XContent
     */
    protected void metadataToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {}

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        Throwable ex = BaseExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            BaseExceptionsHelper.generateThrowableXContent(builder, params, this);
        } else {
            BaseExceptionsHelper.innerToXContent(builder, params, this, getExceptionName(), getMessage(), headers, metadata, getCause());
        }
        return builder;
    }

    protected String getExceptionName() {
        return BaseExceptionsHelper.getExceptionName(this);
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions
     */
    public BaseOpenSearchException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof BaseOpenSearchException) {
            return ((BaseOpenSearchException) cause).guessRootCauses();
        }
        return new BaseOpenSearchException[] { this };
    }

    public void setResources(String type, String... id) {
        assert type != null;
        addMetadata(RESOURCE_METADATA_ID_KEY, id);
        addMetadata(RESOURCE_METADATA_TYPE_KEY, type);
    }

    public List<String> getResourceId() {
        return getMetadata(RESOURCE_METADATA_ID_KEY);
    }

    public String getResourceType() {
        List<String> header = getMetadata(RESOURCE_METADATA_TYPE_KEY);
        if (header != null && header.isEmpty() == false) {
            assert header.size() == 1;
            return header.get(0);
        }
        return null;
    }

    public String getIndexName() {
        List<String> index = getMetadata(INDEX_METADATA_KEY);
        if (index != null && index.isEmpty() == false) {
            return index.get(0);
        }
        return null;
    }

    /**
     * Get index uuid as a string
     *
     * @deprecated remove in favor of Index#toString once Index class is moved to core library
     */
    @Deprecated
    private String getIndexUUID() {
        List<String> index_uuid = getMetadata(INDEX_METADATA_KEY_UUID);
        if (index_uuid != null && index_uuid.isEmpty() == false) {
            return index_uuid.get(0);
        }
        return null;
    }

    /**
     * Returns the rest status code associated with this exception.
     */
    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        } else {
            return BaseExceptionsHelper.status(cause);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (metadata.containsKey(INDEX_METADATA_KEY)) {
            builder.append(getIndexString());
            if (metadata.containsKey(SHARD_METADATA_KEY)) {
                builder.append('[').append(getShardIdString()).append(']');
            }
            builder.append(' ');
        }
        return builder.append(BaseExceptionsHelper.detailedMessage(this).trim()).toString();
    }

    /**
     * Get index string
     *
     * @deprecated remove in favor of Index#toString once Index class is moved to core library
     */
    @Deprecated
    private String getIndexString() {
        String uuid = getIndexUUID();
        if (uuid != null) {
            String name = getIndexName();
            if (Strings.UNKNOWN_UUID_VALUE.equals(uuid)) {
                return "[" + name + "]";
            }
            return "[" + name + "/" + uuid + "]";
        }
        return null;
    }

    /**
     * Get shard id string
     *
     * @deprecated remove in favor of ShardId#toString once ShardId class is moved to core library
     */
    @Deprecated
    private String getShardIdString() {
        String indexName = getIndexName();
        List<String> shard = getMetadata(SHARD_METADATA_KEY);
        if (indexName != null && shard != null && shard.isEmpty() == false) {
            return "[" + indexName + "][" + Integer.parseInt(shard.get(0)) + "]";
        }
        return null;
    }

    /**
     * Returns an array of all registered handle IDs. These are the IDs for every registered
     * exception.
     *
     * @return an array of all registered handle IDs
     */
    static int[] ids() {
        return OpenSearchExceptionHandleRegistry.ids().stream().mapToInt(i -> i).toArray();
    }

    /**
     * Returns an array of all registered pairs of handle IDs and exception classes. These pairs are
     * provided for every registered exception.
     *
     * @return an array of all registered pairs of handle IDs and exception classes
     */
    @SuppressWarnings("unchecked")
    static Tuple<Integer, Class<? extends BaseOpenSearchException>>[] classes() {
        final Tuple<Integer, Class<? extends BaseOpenSearchException>>[] ts = OpenSearchExceptionHandleRegistry.handles()
            .stream()
            .map(h -> Tuple.tuple(h.id, h.exceptionClass))
            .toArray(Tuple[]::new);
        return ts;
    }

    public Index getIndex() {
        List<String> index = getMetadata(INDEX_METADATA_KEY);
        if (index != null && index.isEmpty() == false) {
            List<String> index_uuid = getMetadata(INDEX_METADATA_KEY_UUID);
            return new Index(index.get(0), index_uuid.get(0));
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
            setIndex(new Index(index, Strings.UNKNOWN_UUID_VALUE));
        }
    }

    public ShardId getShardId() {
        List<String> shard = getMetadata(SHARD_METADATA_KEY);
        if (shard != null && shard.isEmpty() == false) {
            return new ShardId(getIndex(), Integer.parseInt(shard.get(0)));
        }
        return null;
    }

    public void setShard(ShardId shardId) {
        if (shardId != null) {
            setIndex(shardId.getIndex());
            addMetadata(SHARD_METADATA_KEY, Integer.toString(shardId.id()));
        }
    }

    /**
     * An ExceptionHandle for registering Exceptions that can be serialized over the transport wire
     *
     * @opensearch.internal
     */
    protected static class BaseOpenSearchExceptionHandle {
        final Class<? extends BaseOpenSearchException> exceptionClass;
        final CheckedFunction<StreamInput, ? extends BaseOpenSearchException, IOException> constructor;
        final int id;
        final Version versionAdded;

        <E extends BaseOpenSearchException> BaseOpenSearchExceptionHandle(
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

    @SuppressWarnings("unchecked")
    public static <T extends StreamInput> BaseOpenSearchException readException(T input, int id) throws IOException {
        CheckedFunction<T, ? extends BaseOpenSearchException, IOException> opensearchException = (CheckedFunction<
            T,
            ? extends BaseOpenSearchException,
            IOException>) OpenSearchExceptionHandleRegistry.getSupplier(id);
        if (opensearchException == null) {
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return opensearchException.apply(input);
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
    public static <S extends StreamOutput, T extends Throwable> T writeStackTraces(
        T throwable,
        StreamOutput out,
        Writer<Throwable> exceptionWriter
    ) throws IOException {
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
     * Returns the serialization id the given exception.
     */
    public static int getId(final Class<? extends BaseOpenSearchException> exception) {
        return OpenSearchExceptionHandleRegistry.getId(exception);
    }

    /**
     * Returns <code>true</code> iff the given class is a registered for an exception to be read.
     */
    public static boolean isRegistered(final Class<? extends Throwable> exception, Version version) {
        return OpenSearchExceptionHandleRegistry.isRegistered(exception, version);
    }

    /**
     * Generate a {@link BaseOpenSearchException} from a {@link XContentParser}. This does not
     * return the original exception type (ie NodeClosedException for example) but just wraps
     * the type, the reason and the cause of the exception. It also recursively parses the
     * tree structure of the cause, returning it as a tree structure of {@link BaseOpenSearchException}
     * instances.
     */
    public static BaseOpenSearchException fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        return innerFromXContent(parser, false);
    }

    /**
     * Parses the output of {@link #generateFailureXContent(XContentBuilder, Params, Exception, boolean)}
     */
    public static BaseOpenSearchException failureFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureFieldName(parser, token, ERROR);

        token = parser.nextToken();
        if (token.isValue()) {
            return new BaseOpenSearchException(buildMessage("exception", parser.text(), null)) {
            };
        }

        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        // Root causes are parsed in the innerFromXContent() and are added as suppressed exceptions.
        return innerFromXContent(parser, true);
    }

    public static BaseOpenSearchException innerFromXContent(XContentParser parser, boolean parseRootCauses) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);

        String type = null, reason = null, stack = null;
        BaseOpenSearchException cause = null;
        Map<String, List<String>> metadata = new HashMap<>();
        Map<String, List<String>> headers = new HashMap<>();
        List<BaseOpenSearchException> rootCauses = new ArrayList<>();
        List<BaseOpenSearchException> suppressed = new ArrayList<>();

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

        BaseOpenSearchException e = new BaseOpenSearchException(buildMessage(type, reason, stack), cause) {
        };
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
        for (BaseOpenSearchException rootCause : rootCauses) {
            e.addSuppressed(rootCause);
        }
        for (BaseOpenSearchException s : suppressed) {
            e.addSuppressed(s);
        }
        return e;
    }

    static Set<Class<? extends BaseOpenSearchException>> getRegisteredKeys() { // for testing
        return OpenSearchExceptionHandleRegistry.getRegisteredKeys();
    }

    /**
     * Registry of ExceptionHandlers
     *
     * @opensearch.internal
     */
    public static class OpenSearchExceptionHandleRegistry {
        /** Registry mapping from unique Ordinal to the Exception Constructor */
        private static final Map<
            Integer,
            CheckedFunction<StreamInput, ? extends BaseOpenSearchException, IOException>> ID_TO_SUPPLIER_REGISTRY =
                new ConcurrentHashMap<>();
        /** Registry mapping from Exception class to the Exception Handler  */
        private static final Map<
            Class<? extends BaseOpenSearchException>,
            BaseOpenSearchExceptionHandle> CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY = new ConcurrentHashMap<>();

        /** returns the Exception constructor function from a given ordinal */
        public static CheckedFunction<StreamInput, ? extends BaseOpenSearchException, IOException> getSupplier(final int id) {
            return ID_TO_SUPPLIER_REGISTRY.get(id);
        }

        /** registers the Exception handler */
        public static void registerExceptionHandle(final BaseOpenSearchExceptionHandle handle) {
            ID_TO_SUPPLIER_REGISTRY.put(handle.id, handle.constructor);
            CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.put(handle.exceptionClass, handle);
        }

        /** Gets the unique ordinal id of the Exception from the given class */
        public static int getId(final Class<? extends BaseOpenSearchException> exception) {
            return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.get(exception).id;
        }

        /** returns a set of ids */
        public static Set<Integer> ids() {
            return ID_TO_SUPPLIER_REGISTRY.keySet();
        }

        /** returns a collection of handles */
        public static Collection<BaseOpenSearchExceptionHandle> handles() {
            return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.values();
        }

        /** checks that the exception class is registered */
        public static boolean isRegistered(final Class<? extends Throwable> exception, final Version version) {
            BaseOpenSearchExceptionHandle openSearchExceptionHandle = CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.get(exception);
            if (openSearchExceptionHandle != null) {
                return version.onOrAfter(openSearchExceptionHandle.versionAdded);
            }
            return false;
        }

        /** returns a set of registered exception classes */
        public static Set<Class<? extends BaseOpenSearchException>> getRegisteredKeys() { // for testing
            return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.keySet();
        }
    }
}
