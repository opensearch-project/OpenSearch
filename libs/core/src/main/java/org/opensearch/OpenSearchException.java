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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;

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

import static java.util.Collections.emptyMap;
import static org.opensearch.OpenSearchException.OpenSearchExceptionHandleRegistry.registerExceptionHandle;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureFieldName;

import static java.util.Collections.singletonMap;

/**
 * A core library base class for all opensearch exceptions.
 *
 * @opensearch.internal
 */
public class OpenSearchException extends RuntimeException implements Writeable, ToXContentFragment {

    protected static final Version UNKNOWN_VERSION_ADDED = Version.fromId(0);

    /**
     * Passed in the {@link ToXContent.Params} of {@link #generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     * to control if the {@code caused_by} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter.
     */
    private static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.cause.skip";
    /**
     * Passed in the {@link ToXContent.Params} of {@link #generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     * to control if the {@code stack_trace} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter. Use the {@code error_trace} parameter instead.
     */
    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";
    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;
    private static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;
    private static final String RESOURCE_METADATA_TYPE_KEY = "opensearch.resource.type";
    private static final String RESOURCE_METADATA_ID_KEY = "opensearch.resource.id";
    private static final String INDEX_METADATA_KEY = "opensearch.index";
    private static final String INDEX_METADATA_KEY_UUID = "opensearch.index_uuid";
    private static final String SHARD_METADATA_KEY = "opensearch.shard";
    private static final String OPENSEARCH_PREFIX_KEY = "opensearch.";

    private static final String TYPE = "type";
    private static final String REASON = "reason";
    private static final String CAUSED_BY = "caused_by";
    private static final ParseField SUPPRESSED = new ParseField("suppressed");
    public static final String STACK_TRACE = "stack_trace";
    private static final String HEADER = "header";
    private static final String ERROR = "error";
    private static final String ROOT_CAUSE = "root_cause";

    protected final Map<String, List<String>> metadata = new HashMap<>();
    protected final Map<String, List<String>> headers = new HashMap<>();

    static {
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.opensearch.core.index.snapshots.IndexShardSnapshotFailedException.class,
                org.opensearch.core.index.snapshots.IndexShardSnapshotFailedException::new,
                0,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.opensearch.OpenSearchParseException.class,
                org.opensearch.OpenSearchParseException::new,
                35,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.opensearch.core.common.ParsingException.class,
                org.opensearch.core.common.ParsingException::new,
                40,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper.class,
                org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper::new,
                62,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new OpenSearchExceptionHandle(
                org.opensearch.core.common.breaker.CircuitBreakingException.class,
                org.opensearch.core.common.breaker.CircuitBreakingException::new,
                133,
                UNKNOWN_VERSION_ADDED
            )
        );
    }

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
    public OpenSearchException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }

    public OpenSearchException(StreamInput in) throws IOException {
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
        if (key.startsWith(OPENSEARCH_PREFIX_KEY) == false) {
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
        if (key.startsWith(OPENSEARCH_PREFIX_KEY)) {
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

    /**
     * Unwraps the actual cause from the exception for cases when the exception is a
     * {@link OpenSearchWrapperException}.
     *
     * @see ExceptionsHelper#unwrapCause(Throwable)
     */
    public Throwable unwrapCause() {
        return ExceptionsHelper.unwrapCause(this);
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(toString()).append("; ");
            if (getCause() instanceof OpenSearchException) {
                sb.append(((OpenSearchException) getCause()).getDetailedMessage());
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

    @SuppressWarnings("unchecked")
    public static <T extends StreamInput> OpenSearchException readException(T input, int id) throws IOException {
        CheckedFunction<T, ? extends OpenSearchException, IOException> opensearchException = (CheckedFunction<
            T,
            ? extends OpenSearchException,
            IOException>) OpenSearchExceptionHandleRegistry.getSupplier(id);
        if (opensearchException == null) {
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return opensearchException.apply(input);
    }

    /**
     * Returns <code>true</code> iff the given class is a registered for an exception to be read.
     */
    public static boolean isRegistered(final Class<? extends Throwable> exception, Version version) {
        return OpenSearchExceptionHandleRegistry.isRegistered(exception, version);
    }

    static Set<Class<? extends OpenSearchException>> getRegisteredKeys() { // for testing
        return OpenSearchExceptionHandleRegistry.getRegisteredKeys();
    }

    /**
     * Returns the serialization id the given exception.
     */
    public static int getId(final Class<? extends OpenSearchException> exception) {
        return OpenSearchExceptionHandleRegistry.getId(exception);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            generateThrowableXContent(builder, params, this);
        } else {
            innerToXContent(builder, params, this, getExceptionName(), getMessage(), headers, metadata, getCause());
        }
        return builder;
    }

    protected static void innerToXContent(
        XContentBuilder builder,
        ToXContent.Params params,
        Throwable throwable,
        String type,
        String message,
        Map<String, List<String>> headers,
        Map<String, List<String>> metadata,
        Throwable cause
    ) throws IOException {
        builder.field(TYPE, type);
        builder.field(REASON, message);

        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            headerToXContent(builder, entry.getKey().substring(OPENSEARCH_PREFIX_KEY.length()), entry.getValue());
        }

        if (throwable instanceof OpenSearchException) {
            OpenSearchException exception = (OpenSearchException) throwable;
            exception.metadataToXContent(builder, params);
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, REST_EXCEPTION_SKIP_CAUSE_DEFAULT) == false) {
            if (cause != null) {
                builder.field(CAUSED_BY);
                builder.startObject();
                generateThrowableXContent(builder, params, cause);
                builder.endObject();
            }
        }

        if (headers.isEmpty() == false) {
            builder.startObject(HEADER);
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                headerToXContent(builder, entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
            builder.field(STACK_TRACE, ExceptionsHelper.stackTrace(throwable));
        }

        Throwable[] allSuppressed = throwable.getSuppressed();
        if (allSuppressed.length > 0) {
            builder.startArray(SUPPRESSED.getPreferredName());
            for (Throwable suppressed : allSuppressed) {
                builder.startObject();
                generateThrowableXContent(builder, params, suppressed);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    protected static void headerToXContent(XContentBuilder builder, String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                builder.field(key, values.get(0));
            } else {
                builder.startArray(key);
                for (String value : values) {
                    builder.value(value);
                }
                builder.endArray();
            }
        }
    }

    /**
     * Renders additional per exception information into the XContent
     */
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {}

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
                if (TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (REASON.equals(currentFieldName)) {
                    reason = parser.text();
                } else if (STACK_TRACE.equals(currentFieldName)) {
                    stack = parser.text();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    metadata.put(currentFieldName, Collections.singletonList(parser.text()));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CAUSED_BY.equals(currentFieldName)) {
                    cause = fromXContent(parser);
                } else if (HEADER.equals(currentFieldName)) {
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
                } else if (SUPPRESSED.match(currentFieldName, parser.getDeprecationHandler())) {
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

        OpenSearchException e = new OpenSearchException(buildMessage(type, reason, stack), cause) {
        };
        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            // subclasses can print out additional metadata through the metadataToXContent method. Simple key-value pairs will be
            // parsed back and become part of this metadata set, while objects and arrays are not supported when parsing back.
            // Those key-value pairs become part of the metadata set and inherit the "opensearch." prefix as that is currently required
            // by addMetadata. The prefix will get stripped out when printing metadata out so it will be effectively invisible.
            // TODO move subclasses that print out simple metadata to using addMetadata directly and support also numbers and booleans.
            // TODO rename metadataToXContent and have only SearchPhaseExecutionException use it, which prints out complex objects
            e.addMetadata(OPENSEARCH_PREFIX_KEY + entry.getKey(), entry.getValue());
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
     * Static toXContent helper method that renders {@link OpenSearchException} or {@link Throwable} instances
     * as XContent, delegating the rendering to {@link OpenSearchException#toXContent(XContentBuilder, ToXContent.Params)}
     * or {@link #innerToXContent(XContentBuilder, ToXContent.Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * This method is usually used when the {@link Throwable} is rendered as a part of another XContent object, and its result can
     * be parsed back using the {@code OpenSearchException.fromXContent(XContentParser)} method.
     */
    public static void generateThrowableXContent(XContentBuilder builder, ToXContent.Params params, Throwable t) throws IOException {
        t = ExceptionsHelper.unwrapCause(t);

        if (t instanceof OpenSearchException) {
            ((OpenSearchException) t).toXContent(builder, params);
        } else {
            innerToXContent(builder, params, t, getExceptionName(t), t.getMessage(), emptyMap(), emptyMap(), t.getCause());
        }
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
                if (t instanceof OpenSearchException) {
                    break;
                }
                t = t.getCause();
            }
            builder.field(ERROR, ExceptionsHelper.summaryMessage(t != null ? t : e));
            return;
        }

        // Render the exception with all details
        final OpenSearchException[] rootCauses = OpenSearchException.guessRootCauses(e);
        builder.startObject(ERROR);
        {
            builder.startArray(ROOT_CAUSE);
            for (OpenSearchException rootCause : rootCauses) {
                builder.startObject();
                rootCause.toXContent(builder, new ToXContent.DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_CAUSE, "true"), params));
                builder.endObject();
            }
            builder.endArray();
        }
        generateThrowableXContent(builder, params, e);
        builder.endObject();
    }

    /**
     * Parses the output of {@link #generateFailureXContent(XContentBuilder, Params, Exception, boolean)}
     */
    public static OpenSearchException failureFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureFieldName(parser, token, ERROR);

        token = parser.nextToken();
        if (token.isValue()) {
            return new OpenSearchException(buildMessage("exception", parser.text(), null)) {
            };
        }

        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();

        // Root causes are parsed in the innerFromXContent() and are added as suppressed exceptions.
        return innerFromXContent(parser, true);
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions
     */
    public OpenSearchException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof OpenSearchException) {
            return ((OpenSearchException) cause).guessRootCauses();
        }
        return new OpenSearchException[] { this };
    }

    /**
     * Returns the root cause of this exception or multiple if different shards caused different exceptions.
     * If the given exception is not an instance of {@link OpenSearchException} an empty array
     * is returned.
     */
    public static OpenSearchException[] guessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof OpenSearchException) {
            // OpenSearchException knows how to guess its own root cause
            return ((OpenSearchException) ex).guessRootCauses();
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
                if (cause instanceof XContentParseException || cause instanceof OpenSearchException) {
                    return OpenSearchException.guessRootCauses(ex.getCause());
                }
            }
        }
        return new OpenSearchException[] { new OpenSearchException(ex.getMessage(), ex) {
            @Override
            protected String getExceptionName() {
                return getExceptionName(getCause());
            }
        } };
    }

    protected String getExceptionName() {
        return getExceptionName(this);
    }

    /**
     * Returns an underscore case name for the given exception. This method strips {@code OpenSearch} prefixes from exception names.
     */
    public static String getExceptionName(Throwable ex) {
        String simpleName = getExceptionSimpleClassName(ex);
        if (simpleName.startsWith("OpenSearch")) {
            simpleName = simpleName.substring("OpenSearch".length());
        }
        // TODO: do we really need to make the exception name in underscore casing?
        return toUnderscoreCase(simpleName);
    }

    public static String getExceptionSimpleClassName(final Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (Strings.isEmpty(simpleName)) {
            simpleName = "OpenSearchException";
        }
        return simpleName;
    }

    static String buildMessage(String type, String reason, String stack) {
        StringBuilder message = new StringBuilder("OpenSearch exception [");
        message.append(TYPE).append('=').append(type).append(", ");
        message.append(REASON).append('=').append(reason);
        if (stack != null) {
            message.append(", ").append(STACK_TRACE).append('=').append(stack);
        }
        message.append(']');
        return message.toString();
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
        return builder.append(ExceptionsHelper.detailedMessage(this).trim()).toString();
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

    // lower cases and adds underscores to transitions in a name
    private static String toUnderscoreCase(String value) {
        StringBuilder sb = new StringBuilder();
        boolean changed = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isUpperCase(c)) {
                if (!changed) {
                    // copy it over here
                    for (int j = 0; j < i; j++) {
                        sb.append(value.charAt(j));
                    }
                    changed = true;
                    if (i == 0) {
                        sb.append(Character.toLowerCase(c));
                    } else {
                        sb.append('_');
                        sb.append(Character.toLowerCase(c));
                    }
                } else {
                    sb.append('_');
                    sb.append(Character.toLowerCase(c));
                }
            } else {
                if (changed) {
                    sb.append(c);
                }
            }
        }
        if (!changed) {
            return value;
        }
        return sb.toString();
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
    static Tuple<Integer, Class<? extends OpenSearchException>>[] classes() {
        final Tuple<Integer, Class<? extends OpenSearchException>>[] ts = OpenSearchExceptionHandleRegistry.handles()
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
     * This is the list of Exceptions OpenSearch can throw over the wire or save into a corruption marker. Each value in the enum is a
     * single exception tying the Class to an id for use of the encode side and the id back to a constructor for use on the decode side. As
     * such its ok if the exceptions to change names so long as their constructor can still read the exception. Each exception is listed
     * in id order. If you want to remove an exception leave a tombstone comment and mark the id as null in
     * ExceptionSerializationTests.testIds.ids.
     *
     * @opensearch.internal
     */
    protected static class OpenSearchExceptionHandle {
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
     * Registry of ExceptionHandlers
     *
     * @opensearch.internal
     */
    public static class OpenSearchExceptionHandleRegistry {
        /** Registry mapping from unique Ordinal to the Exception Constructor */
        private static final Map<
            Integer,
            CheckedFunction<StreamInput, ? extends OpenSearchException, IOException>> ID_TO_SUPPLIER_REGISTRY = new ConcurrentHashMap<>();
        /** Registry mapping from Exception class to the Exception Handler  */
        private static final Map<
            Class<? extends OpenSearchException>,
            OpenSearchExceptionHandle> CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY = new ConcurrentHashMap<>();

        /** returns the Exception constructor function from a given ordinal */
        public static CheckedFunction<StreamInput, ? extends OpenSearchException, IOException> getSupplier(final int id) {
            return ID_TO_SUPPLIER_REGISTRY.get(id);
        }

        /** registers the Exception handler */
        public static void registerExceptionHandle(final OpenSearchExceptionHandle handle) {
            ID_TO_SUPPLIER_REGISTRY.put(handle.id, handle.constructor);
            CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.put(handle.exceptionClass, handle);
        }

        /** Gets the unique ordinal id of the Exception from the given class */
        public static int getId(final Class<? extends OpenSearchException> exception) {
            return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.get(exception).id;
        }

        /** returns a set of ids */
        public static Set<Integer> ids() {
            return ID_TO_SUPPLIER_REGISTRY.keySet();
        }

        /** returns a collection of handles */
        public static Collection<OpenSearchExceptionHandle> handles() {
            return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.values();
        }

        /** checks that the exception class is registered */
        public static boolean isRegistered(final Class<? extends Throwable> exception, final Version version) {
            OpenSearchExceptionHandle openSearchExceptionHandle = CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.get(exception);
            if (openSearchExceptionHandle != null) {
                return version.onOrAfter(openSearchExceptionHandle.versionAdded);
            }
            return false;
        }

        /** returns a set of registered exception classes */
        public static Set<Class<? extends OpenSearchException>> getRegisteredKeys() { // for testing
            return CLASS_TO_OPENSEARCH_EXCEPTION_HANDLE_REGISTRY.keySet();
        }
    }
}
