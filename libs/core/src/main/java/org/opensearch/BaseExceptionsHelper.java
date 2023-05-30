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

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.ParseField;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Base helper class for OpenSearch Exceptions
 *
 * @opensearch.internal
 */
public abstract class BaseExceptionsHelper {
    /**
     * Passed in the {@link ToXContent.Params} of {@link #generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     * to control if the {@code stack_trace} element should render. Unlike most parameters to {@code toXContent} methods this parameter is
     * internal only and not available as a URL parameter. Use the {@code error_trace} parameter instead.
     */
    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";
    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;
    public static final String STACK_TRACE = "stack_trace";
    protected static final Logger logger = LogManager.getLogger(BaseExceptionsHelper.class);
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
    private static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;

    public static Throwable unwrapCause(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof OpenSearchWrapperException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter++ > 10) {
                // dear god, if we got more than 10 levels down, WTF? just bail
                logger.warn("Exception cause unwrapping ran for 10 levels...", t);
                return result;
            }
            result = result.getCause();
        }
        return result;
    }

    /**
     * @deprecated Don't swallow exceptions, allow them to propagate.
     */
    @Deprecated
    public static String detailedMessage(Throwable t) {
        if (t == null) {
            return "Unknown";
        }
        if (t.getCause() != null) {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(t.getClass().getSimpleName());
                if (t.getMessage() != null) {
                    sb.append("[");
                    sb.append(t.getMessage());
                    sb.append("]");
                }
                sb.append("; ");
                t = t.getCause();
                if (t != null) {
                    sb.append("nested: ");
                }
            }
            return sb.toString();
        } else {
            return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
        }
    }

    public static String stackTrace(Throwable e) {
        StringWriter stackTraceStringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
        e.printStackTrace(printWriter);
        return stackTraceStringWriter.toString();
    }

    public static String summaryMessage(Throwable t) {
        if (t != null) {
            if (t instanceof BaseOpenSearchException) {
                return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
            } else if (t instanceof IllegalArgumentException) {
                return "Invalid argument";
            } else if (t instanceof JsonParseException) {
                return "Failed to parse JSON";
            } else if (t instanceof OpenSearchRejectedExecutionException) {
                return "Too many requests";
            }
        }
        return "Internal failure";
    }

    public static void innerToXContent(
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
            builder.field(STACK_TRACE, stackTrace(throwable));
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

    /**
     * Static toXContent helper method that renders {@link BaseOpenSearchException} or {@link Throwable} instances
     * as XContent, delegating the rendering to {@link BaseOpenSearchException#toXContent(XContentBuilder, ToXContent.Params)}
     * or {@link #innerToXContent(XContentBuilder, ToXContent.Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * This method is usually used when the {@link Throwable} is rendered as a part of another XContent object, and its result can
     * be parsed back using the {@code OpenSearchException.fromXContent(XContentParser)} method.
     */
    public static void generateThrowableXContent(XContentBuilder builder, ToXContent.Params params, Throwable t) throws IOException {
        t = unwrapCause(t);

        if (t instanceof BaseOpenSearchException) {
            ((BaseOpenSearchException) t).toXContent(builder, params);
        } else {
            innerToXContent(builder, params, t, getExceptionName(t), t.getMessage(), emptyMap(), emptyMap(), t.getCause());
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
        return toUnderscoreCase(simpleName);
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
}
