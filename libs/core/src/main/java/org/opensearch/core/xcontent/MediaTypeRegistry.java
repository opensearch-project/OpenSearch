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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parses supported internet media types
 *
 * @opensearch.internal
 */
public final class MediaTypeRegistry {
    private static Map<String, MediaType> formatToMediaType = Map.of();
    private static Map<String, MediaType> typeWithSubtypeToMediaType = Map.of();

    // Default mediaType singleton
    private static MediaType DEFAULT_MEDIA_TYPE;

    public static void register(MediaType[] acceptedMediaTypes, Map<String, MediaType> additionalMediaTypes) {
        // ensures the map is not overwritten:
        Map<String, MediaType> typeMap = new HashMap<>(typeWithSubtypeToMediaType);
        Map<String, MediaType> formatMap = new HashMap<>(formatToMediaType);
        for (MediaType mediaType : acceptedMediaTypes) {
            if (formatMap.containsKey(mediaType.format())) {
                throw new IllegalArgumentException("unable to register mediaType: [" + mediaType.format() + "]. Type already exists.");
            }
            typeMap.put(mediaType.typeWithSubtype(), mediaType);
            formatMap.put(mediaType.format(), mediaType);
        }
        for (Map.Entry<String, MediaType> entry : additionalMediaTypes.entrySet()) {
            String typeWithSubtype = entry.getKey().toLowerCase(Locale.ROOT);
            if (typeMap.containsKey(typeWithSubtype)) {
                throw new IllegalArgumentException(
                    "unable to register mediaType: ["
                        + entry.getKey()
                        + "]. "
                        + "Type already exists and is mapped to: [."
                        + entry.getValue().format()
                        + "]"
                );
            }

            MediaType mediaType = entry.getValue();
            typeMap.put(typeWithSubtype, mediaType);
            formatMap.putIfAbsent(mediaType.format(), mediaType); // ignore if the additional type mapping already exists
        }

        formatToMediaType = Map.copyOf(formatMap);
        typeWithSubtypeToMediaType = Map.copyOf(typeMap);
    }

    public static MediaType fromMediaType(String mediaType) {
        ParsedMediaType parsedMediaType = parseMediaType(mediaType);
        return parsedMediaType != null ? parsedMediaType.getMediaType() : null;
    }

    public static MediaType fromFormat(String format) {
        if (format == null) {
            return null;
        }
        return formatToMediaType.get(format.toLowerCase(Locale.ROOT));
    }

    /**
     * parsing media type that follows https://tools.ietf.org/html/rfc7231#section-3.1.1.1
     * @param headerValue a header value from Accept or Content-Type
     * @return a parsed media-type
     */
    public static ParsedMediaType parseMediaType(String headerValue) {
        if (headerValue != null) {
            String[] split = headerValue.toLowerCase(Locale.ROOT).split(";");

            String[] typeSubtype = split[0].trim().split("/");
            if (typeSubtype.length == 2) {
                String type = typeSubtype[0];
                String subtype = typeSubtype[1];
                MediaType mediaType = typeWithSubtypeToMediaType.get(type + "/" + subtype);
                if (mediaType != null) {
                    Map<String, String> parameters = new HashMap<>();
                    for (int i = 1; i < split.length; i++) {
                        // spaces are allowed between parameters, but not between '=' sign
                        String[] keyValueParam = split[i].trim().split("=");
                        if (keyValueParam.length != 2 || hasSpaces(keyValueParam[0]) || hasSpaces(keyValueParam[1])) {
                            return null;
                        }
                        parameters.put(keyValueParam[0], keyValueParam[1]);
                    }
                    return new ParsedMediaType(mediaType, parameters);
                }
            }

        }
        return null;
    }

    private static boolean hasSpaces(String s) {
        return s.trim().equals(s) == false;
    }

    /**
     * A media type object that contains all the information provided on a Content-Type or Accept header
     */
    public static class ParsedMediaType {
        private final Map<String, String> parameters;
        private final MediaType mediaType;

        public ParsedMediaType(MediaType mediaType, Map<String, String> parameters) {
            this.parameters = parameters;
            this.mediaType = mediaType;
        }

        public MediaType getMediaType() {
            return mediaType;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }
    }

    public static void setDefaultMediaType(final MediaType mediaType) {
        if (DEFAULT_MEDIA_TYPE != null) {
            throw new RuntimeException(
                "unable to reset the default media type from current default [" + DEFAULT_MEDIA_TYPE + "] to [" + mediaType + "]"
            );
        } else {
            DEFAULT_MEDIA_TYPE = mediaType;
        }
    }

    public static MediaType getDefaultMediaType() {
        return DEFAULT_MEDIA_TYPE;
    }
}
