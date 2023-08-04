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

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A one stop to use {@link XContent} and {@link XContentBuilder}.
 */
public class XContentFactory {

    /**
     * Returns a content builder using JSON format ({@link org.opensearch.common.xcontent.XContentType#JSON}.
     */
    public static XContentBuilder jsonBuilder() throws IOException {
        return MediaTypeRegistry.contentBuilder(XContentType.JSON);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder jsonBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(JsonXContent.jsonXContent, os);
    }

    /**
     * Returns a content builder using SMILE format ({@link org.opensearch.common.xcontent.XContentType#SMILE}.
     */
    public static XContentBuilder smileBuilder() throws IOException {
        return MediaTypeRegistry.contentBuilder(XContentType.SMILE);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder smileBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(SmileXContent.smileXContent, os);
    }

    /**
     * Returns a content builder using YAML format ({@link org.opensearch.common.xcontent.XContentType#YAML}.
     */
    public static XContentBuilder yamlBuilder() throws IOException {
        return MediaTypeRegistry.contentBuilder(XContentType.YAML);
    }

    /**
     * Constructs a new yaml builder that will output the result into the provided output stream.
     */
    public static XContentBuilder yamlBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(YamlXContent.yamlXContent, os);
    }

    /**
     * Returns a content builder using CBOR format ({@link org.opensearch.common.xcontent.XContentType#CBOR}.
     */
    public static XContentBuilder cborBuilder() throws IOException {
        return MediaTypeRegistry.contentBuilder(XContentType.CBOR);
    }
}
