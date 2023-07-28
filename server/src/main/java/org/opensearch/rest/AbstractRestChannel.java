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

package org.opensearch.rest;

import org.opensearch.common.Nullable;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

/**
 * Base REST channel
 *
 * @opensearch.api
 */
public abstract class AbstractRestChannel implements RestChannel {

    private static final Predicate<String> INCLUDE_FILTER = f -> f.charAt(0) != '-';
    private static final Predicate<String> EXCLUDE_FILTER = INCLUDE_FILTER.negate();

    protected final RestRequest request;
    private final boolean detailedErrorsEnabled;
    private final String format;
    private final String filterPath;
    private final boolean pretty;
    private final boolean human;
    private final String acceptHeader;

    private BytesStreamOutput bytesOut;

    /**
     * Construct a channel for handling the request.
     *
     * @param request               the request
     * @param detailedErrorsEnabled if detailed errors should be reported to the channel
     * @throws IllegalArgumentException if parsing the pretty or human parameters fails
     */
    protected AbstractRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
        this.request = request;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.format = request.param("format");
        this.acceptHeader = request.header("Accept");
        this.filterPath = request.param("filter_path", null);
        this.pretty = request.paramAsBoolean("pretty", false);
        this.human = request.paramAsBoolean("human", false);
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return newBuilder(request.getMediaType(), true);
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        // Disable filtering when building error responses
        return newBuilder(request.getMediaType(), false);
    }

    /**
     * Creates a new {@link XContentBuilder} for a response to be sent using this channel. The builder's type is determined by the following
     * logic. If the request has a format parameter that will be used to attempt to map to an {@link MediaType}. If there is no format
     * parameter, the HTTP Accept header is checked to see if it can be matched to a {@link MediaType}. If this first attempt to map
     * fails, the request content type will be used if the value is not {@code null}; if the value is {@code null} the output format falls
     * back to JSON.
     */
    @Override
    public XContentBuilder newBuilder(@Nullable MediaType requestContentType, boolean useFiltering) throws IOException {
        return newBuilder(requestContentType, null, useFiltering);
    }

    /**
     * Creates a new {@link XContentBuilder} for a response to be sent using this channel. The builder's type can be sent as a parameter,
     * through {@code responseContentType} or it can fallback to {@link #newBuilder(MediaType, boolean)} logic if the sent type value
     * is {@code null}.
     */
    @Override
    public XContentBuilder newBuilder(@Nullable MediaType requestContentType, @Nullable MediaType responseContentType, boolean useFiltering)
        throws IOException {
        if (responseContentType == null) {
            // TODO should format vs acceptHeader always be the same, do we allow overriding?
            responseContentType = MediaType.fromFormat(format);
            if (responseContentType == null) {
                responseContentType = MediaType.fromMediaType(acceptHeader);
            }
        }
        // try to determine the response content type from the media type or the format query string parameter, with the format parameter
        // taking precedence over the Accept header
        if (responseContentType == null) {
            if (requestContentType != null) {
                // if there was a parsed content-type for the incoming request use that since no format was specified using the query
                // string parameter or the HTTP Accept header
                responseContentType = requestContentType;
            } else {
                // default to JSON output when all else fails
                responseContentType = MediaTypeRegistry.getDefaultMediaType();
            }
        }

        Set<String> includes = Collections.emptySet();
        Set<String> excludes = Collections.emptySet();
        if (useFiltering) {
            Set<String> filters = Strings.tokenizeByCommaToSet(filterPath);
            includes = filters.stream().filter(INCLUDE_FILTER).collect(toSet());
            excludes = filters.stream().filter(EXCLUDE_FILTER).map(f -> f.substring(1)).collect(toSet());
        }

        OutputStream unclosableOutputStream = Streams.flushOnCloseStream(bytesOutput());
        XContentBuilder builder = new XContentBuilder(responseContentType.xContent(), unclosableOutputStream, includes, excludes);
        if (pretty) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.humanReadable(human);
        return builder;
    }

    /**
     * A channel level bytes output that can be reused. The bytes output is lazily instantiated
     * by a call to {@link #newBytesOutput()}. Once the stream is created, it gets reset on each
     * call to this method.
     */
    @Override
    public final BytesStreamOutput bytesOutput() {
        if (bytesOut == null) {
            bytesOut = newBytesOutput();
        } else {
            bytesOut.reset();
        }
        return bytesOut;
    }

    /**
     * An accessor to the raw value of the channel bytes output. This method will not instantiate
     * a new stream if one does not exist and this method will not reset the stream.
     */
    protected final BytesStreamOutput bytesOutputOrNull() {
        return bytesOut;
    }

    protected BytesStreamOutput newBytesOutput() {
        return new BytesStreamOutput();
    }

    @Override
    public RestRequest request() {
        return this.request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }
}
