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

package org.opensearch.core.xcontent;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;

/**
 * Abstracts a <a href="http://en.wikipedia.org/wiki/Internet_media_type">Media Type</a> and a format parameter.
 * Media types are used as values on Content-Type and Accept headers
 * format is an URL parameter, specifies response media type.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.1.0")
public interface MediaType extends Writeable {
    /**
     * Returns a type part of a MediaType
     * i.e. application for application/json
     */
    String type();

    /**
     * Returns a subtype part of a MediaType.
     * i.e. json for application/json
     */
    String subtype();

    /**
     * Returns a corresponding format for a MediaType. i.e. json for application/json media type
     * Can differ from the MediaType's subtype i.e plain/text has a subtype of text but format is txt
     */
    String format();

    /**
     * returns a string representation of a media type.
     */
    default String typeWithSubtype() {
        return type() + "/" + subtype();
    }

    /**
     * Unique identifier typically used for binary serialization. Must be distinct
     * from the unique IDs of all other MediaTypes registered with {@link MediaTypeRegistry}.
     * See {@link MediaType#readFrom} and {@link MediaType#writeTo}.
     */
    int uniqueId();

    XContent xContent();

    boolean detectedXContent(final byte[] bytes, int offset, int length);

    boolean detectedXContent(final CharSequence content, final int length);

    default String mediaType() {
        return mediaTypeWithoutParameters();
    }

    String mediaTypeWithoutParameters();

    XContentBuilder contentBuilder() throws IOException;

    XContentBuilder contentBuilder(final OutputStream os) throws IOException;

    /**
     * Serializes this MediaType to the given StreamOutput.
     */
    @Override
    default void writeTo(StreamOutput output) throws IOException {
        if (output.getVersion().onOrAfter(Version.V_2_10_0) && output.getVersion().before(Version.V_3_0_0)) {
            output.writeString(this.mediaType());
        } else {
            output.writeVInt(uniqueId());
        }
    }

    /**
     * Reads a MediaType instance from the given StreamInput.
     */
    static MediaType readFrom(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_2_10_0) && in.getVersion().before(Version.V_3_0_0)) {
            return MediaTypeRegistry.fromMediaType(in.readString());
        } else {
            return MediaTypeRegistry.fromUniqueId(in.readVInt());
        }
    }

    /**
     * Accepts a format string, which is most of the time is equivalent to {@link MediaType#subtype()}
     * and attempts to match the value to an {@link MediaType}.
     * The comparisons are done in lower case format.
     * This method will return {@code null} if no match is found
     */
    static MediaType fromFormat(String mediaType) {
        return MediaTypeRegistry.fromFormat(mediaType);
    }

    /**
     * Attempts to match the given media type with the known {@link MediaType} values. This match is done in a case-insensitive manner.
     * The provided media type can optionally has parameters.
     * This method is suitable for parsing of the {@code Content-Type} and {@code Accept} HTTP headers.
     * This method will return {@code null} if no match is found
     */
    static MediaType fromMediaType(String mediaTypeHeaderValue) {
        mediaTypeHeaderValue = removeVersionInMediaType(mediaTypeHeaderValue);
        return MediaTypeRegistry.fromMediaType(mediaTypeHeaderValue);
    }

    /**
     * Clients compatible with ES 7.x might start sending media types with versioned media type
     * in a form of application/vnd.elasticsearch+json;compatible-with=7.
     * This has to be removed in order to be used in 7.x server.
     * The same client connecting using that media type will be able to communicate with ES 8 thanks to compatible API.
     * @param mediaType - a media type used on Content-Type header, might contain versioned media type.
     *
     * @return a media type string without
     */
    private static String removeVersionInMediaType(String mediaType) {
        if (mediaType != null && (mediaType = mediaType.toLowerCase(Locale.ROOT)).contains("vnd.opensearch")) {
            return mediaType.replaceAll("vnd.opensearch\\+", "").replaceAll("\\s*;\\s*compatible-with=\\d+", "");
        }
        return mediaType;
    }
}
