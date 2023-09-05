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

package org.opensearch.common.xcontent;

import org.opensearch.common.xcontent.cbor.CborXContent;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeParser;
import org.opensearch.core.xcontent.XContent;

import java.util.Locale;
import java.util.Map;

/**
 * The content type of {@link XContent}.
 */
public enum XContentType implements MediaType {

    /**
     * A JSON based content type.
     */
    JSON(0) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/json";
        }

        @Override
        public String mediaType() {
            return "application/json; charset=UTF-8";
        }

        @Override
        public String subtype() {
            return "json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }
    },
    /**
     * The jackson based smile binary format. Fast and compact binary format.
     */
    SMILE(1) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/smile";
        }

        @Override
        public String subtype() {
            return "smile";
        }

        @Override
        public XContent xContent() {
            return SmileXContent.smileXContent;
        }
    },
    /**
     * A YAML based content type.
     */
    YAML(2) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/yaml";
        }

        @Override
        public String subtype() {
            return "yaml";
        }

        @Override
        public XContent xContent() {
            return YamlXContent.yamlXContent;
        }
    },
    /**
     * A CBOR based content type.
     */
    CBOR(3) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/cbor";
        }

        @Override
        public String subtype() {
            return "cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }
    };

    /** a parser of media types */
    private static final MediaTypeParser<XContentType> MEDIA_TYPE_PARSER = new MediaTypeParser<>(
        XContentType.values(),
        Map.of("application/*", JSON, "application/x-ndjson", JSON)
    );

    /** gets the {@link MediaTypeParser} singleton for use outside class */
    @SuppressWarnings("rawtypes")
    public static MediaTypeParser getMediaTypeParser() {
        return MEDIA_TYPE_PARSER;
    }

    /**
     * Accepts a format string, which is most of the time is equivalent to {@link XContentType#subtype()}
     * and attempts to match the value to an {@link XContentType}.
     * The comparisons are done in lower case format.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromFormat(String mediaType) {
        return MEDIA_TYPE_PARSER.fromFormat(mediaType);
    }

    /**
     * Attempts to match the given media type with the known {@link XContentType} values. This match is done in a case-insensitive manner.
     * The provided media type can optionally has parameters.
     * This method is suitable for parsing of the {@code Content-Type} and {@code Accept} HTTP headers.
     * This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaType(String mediaTypeHeaderValue) {
        mediaTypeHeaderValue = removeVersionInMediaType(mediaTypeHeaderValue);
        return MEDIA_TYPE_PARSER.fromMediaType(mediaTypeHeaderValue);
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

    private int index;

    XContentType(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    @Override
    public String type() {
        return "application";
    }

    @Override
    public String format() {
        return subtype();
    }

    /** Converts from a {@link MediaType} to an explicit {@link XContentType} */
    public static XContentType fromMediaType(MediaType mediaType) {
        if (mediaType instanceof XContentType) {
            return (XContentType) mediaType;
        } else {
            return mediaType != null ? MEDIA_TYPE_PARSER.fromMediaType(mediaType.mediaTypeWithoutParameters()) : null;
        }
    }
}
