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

package org.opensearch.common;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * String utility class.
 *
 * @opensearch.internal
 */
public class Strings {

    public static final String[] EMPTY_ARRAY = org.opensearch.core.common.Strings.EMPTY_ARRAY;

    private Strings() {}

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. The content is not pretty-printed
     * nor human readable.
     */
    public static String toString(MediaType mediaType, ToXContent toXContent) {
        return toString(mediaType, toXContent, false, false);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * The content is not pretty-printed nor human readable.
     */
    public static String toString(MediaType mediaType, ToXContent toXContent, ToXContent.Params params) {
        return toString(mediaType, toXContent, params, false, false);
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     * @param xContentBuilder builder containing an object to converted to a string
     */
    public static String toString(XContentBuilder xContentBuilder) {
        return BytesReference.bytes(xContentBuilder).utf8ToString();
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. Allows to control whether the outputted
     * json needs to be pretty printed and human readable.
     *
     */
    public static String toString(MediaType mediaType, ToXContent toXContent, boolean pretty, boolean human) {
        return toString(mediaType, toXContent, ToXContent.EMPTY_PARAMS, pretty, human);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * Allows to control whether the outputted json needs to be pretty printed and human readable.
     */
    private static String toString(MediaType mediaType, ToXContent toXContent, ToXContent.Params params, boolean pretty, boolean human) {
        try {
            XContentBuilder builder = createBuilder(mediaType, pretty, human);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return toString(builder);
        } catch (IOException e) {
            try {
                XContentBuilder builder = createBuilder(mediaType, pretty, human);
                builder.startObject();
                builder.field("error", "error building toString out of XContent: " + e.getMessage());
                builder.field("stack_trace", ExceptionsHelper.stackTrace(e));
                builder.endObject();
                return toString(builder);
            } catch (IOException e2) {
                throw new OpenSearchException("cannot generate error message for deserialization", e);
            }
        }
    }

    private static XContentBuilder createBuilder(MediaType mediaType, boolean pretty, boolean human) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(mediaType.xContent());
        if (pretty) {
            builder.prettyPrint();
        }
        if (human) {
            builder.humanReadable(true);
        }
        return builder;
    }
}
