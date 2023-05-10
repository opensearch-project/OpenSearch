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

import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * A core library base class for all opensearch exceptions.
 *
 * @opensearch.internal
 */
public abstract class BaseOpenSearchException extends RuntimeException {

    /**
     * Passed in the {@link ToXContent.Params} of {@link #generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     * to control if the {@code stack_trace} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter. Use the {@code error_trace} parameter instead.
     */
    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";
    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;
    public static final String STACK_TRACE = "stack_trace";
    protected static final String OPENSEARCH_PREFIX_KEY = "opensearch.";
    /**
     * Passed in the {@link ToXContent.Params} of {@link #generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     * to control if the {@code caused_by} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter.
     */
    protected static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.cause.skip";
    protected static final String TYPE = "type";
    protected static final String REASON = "reason";
    protected static final String CAUSED_BY = "caused_by";
    protected static final ParseField SUPPRESSED = new ParseField("suppressed");
    protected static final String HEADER = "header";
    protected static final String ERROR = "error";
    protected static final String ROOT_CAUSE = "root_cause";
    protected static final String RESOURCE_METADATA_TYPE_KEY = "opensearch.resource.type";
    protected static final String RESOURCE_METADATA_ID_KEY = "opensearch.resource.id";
    protected static final Version UNKNOWN_VERSION_ADDED = Version.fromId(0);
    protected static final String INDEX_METADATA_KEY = "opensearch.index";
    protected static final String SHARD_METADATA_KEY = "opensearch.shard";
    protected static final String INDEX_METADATA_KEY_UUID = "opensearch.index_uuid";
    private static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;
    protected final Map<String, List<String>> metadata = new HashMap<>();
    protected final Map<String, List<String>> headers = new HashMap<>();

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

        if (throwable instanceof BaseOpenSearchException) {
            BaseOpenSearchException exception = (BaseOpenSearchException) throwable;
            exception.metadataToXContent(builder, params);
        }

        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, REST_EXCEPTION_SKIP_CAUSE_DEFAULT) == false) {
            if (cause != null) {
                builder.field(CAUSED_BY);
                builder.startObject();
                BaseOpenSearchException.generateThrowableXContent(builder, params, cause);
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
            builder.field(STACK_TRACE, BaseExceptionsHelper.stackTrace(throwable));
        }

        Throwable[] allSuppressed = throwable.getSuppressed();
        if (allSuppressed.length > 0) {
            builder.startArray(SUPPRESSED.getPreferredName());
            for (Throwable suppressed : allSuppressed) {
                builder.startObject();
                BaseOpenSearchException.generateThrowableXContent(builder, params, suppressed);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    /**
     * Static toXContent helper method that renders {@link BaseOpenSearchException} or {@link Throwable} instances
     * as XContent, delegating the rendering to {@link #toXContent(XContentBuilder, ToXContent.Params)}
     * or {@link #innerToXContent(XContentBuilder, ToXContent.Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * This method is usually used when the {@link Throwable} is rendered as a part of another XContent object, and its result can
     * be parsed back using the {@code OpenSearchException.fromXContent(XContentParser)} method.
     */
    public static void generateThrowableXContent(XContentBuilder builder, ToXContent.Params params, Throwable t) throws IOException {
        t = BaseExceptionsHelper.unwrapCause(t);

        if (t instanceof BaseOpenSearchException) {
            ((BaseOpenSearchException) t).toXContent(builder, params);
        } else {
            BaseOpenSearchException.innerToXContent(
                builder,
                params,
                t,
                BaseOpenSearchException.getExceptionName(t),
                t.getMessage(),
                emptyMap(),
                emptyMap(),
                t.getCause()
            );
        }
    }

    /**
     * Returns an underscore case name for the given exception. This method strips {@code OpenSearch} prefixes from exception names.
     */
    public static String getExceptionName(Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (simpleName.startsWith("OpenSearch")) {
            simpleName = simpleName.substring("OpenSearch".length());
        }
        // TODO: do we really need to make the exception name in underscore casing?
        return BaseOpenSearchException.toUnderscoreCase(simpleName);
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
                rootCause.toXContent(builder, new ToXContent.DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_CAUSE, "true"), params));
                builder.endObject();
            }
            builder.endArray();
        }
        generateThrowableXContent(builder, params, e);
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
                return BaseOpenSearchException.getExceptionName(getCause());
            }
        } };
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

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        Throwable ex = BaseExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            generateThrowableXContent(builder, params, this);
        } else {
            innerToXContent(builder, params, this, getExceptionName(), getMessage(), headers, metadata, getCause());
        }
        return builder;
    }

    protected String getExceptionName() {
        return getExceptionName(this);
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
}
