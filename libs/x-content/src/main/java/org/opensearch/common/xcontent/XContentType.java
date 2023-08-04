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

import com.fasterxml.jackson.dataformat.cbor.CBORConstants;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import org.opensearch.common.xcontent.cbor.CborXContent;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;

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

        @Override
        public boolean detectedXContent(final byte[] bytes, int offset, int length) {
            return bytes[offset] == '{';
        }

        @Override
        public boolean detectedXContent(final CharSequence content, final int length) {
            return content.charAt(0) == '{';
        }

        @Override
        public XContentBuilder contentBuilder() throws IOException {
            return JsonXContent.contentBuilder();
        }

        @Override
        public XContentBuilder contentBuilder(final OutputStream os) throws IOException {
            return new XContentBuilder(JsonXContent.jsonXContent, os);
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

        @Override
        public boolean detectedXContent(final byte[] bytes, int offset, int length) {
            return length > 2
                && bytes[offset] == SmileConstants.HEADER_BYTE_1
                && bytes[offset + 1] == SmileConstants.HEADER_BYTE_2
                && bytes[offset + 2] == SmileConstants.HEADER_BYTE_3;
        }

        @Override
        public boolean detectedXContent(final CharSequence content, final int length) {
            return length > 2
                && content.charAt(0) == SmileConstants.HEADER_BYTE_1
                && content.charAt(1) == SmileConstants.HEADER_BYTE_2
                && content.charAt(2) == SmileConstants.HEADER_BYTE_3;
        }

        @Override
        public XContentBuilder contentBuilder() throws IOException {
            return SmileXContent.contentBuilder();
        }

        @Override
        public XContentBuilder contentBuilder(final OutputStream os) throws IOException {
            return new XContentBuilder(SmileXContent.smileXContent, os);
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

        @Override
        public boolean detectedXContent(final byte[] bytes, int offset, int length) {
            return length > 2 && bytes[offset] == '-' && bytes[offset + 1] == '-' && bytes[offset + 2] == '-';
        }

        @Override
        public boolean detectedXContent(final CharSequence content, final int length) {
            return length > 2 && content.charAt(0) == '-' && content.charAt(1) == '-' && content.charAt(2) == '-';
        }

        @Override
        public XContentBuilder contentBuilder() throws IOException {
            return YamlXContent.contentBuilder();
        }

        @Override
        public XContentBuilder contentBuilder(final OutputStream os) throws IOException {
            return new XContentBuilder(YamlXContent.yamlXContent, os);
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

        @Override
        public boolean detectedXContent(final byte[] bytes, int offset, int length) {
            // CBOR logic similar to CBORFactory#hasCBORFormat
            if (bytes[offset] == CBORConstants.BYTE_OBJECT_INDEFINITE && length > 1) {
                return true;
            }
            if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_TAG, bytes[offset]) && length > 2) {
                // Actually, specific "self-describe tag" is a very good indicator
                if (bytes[offset] == (byte) 0xD9 && bytes[offset + 1] == (byte) 0xD9 && bytes[offset + 2] == (byte) 0xF7) {
                    return true;
                }
            }
            // for small objects, some encoders just encode as major type object, we can safely
            // say its CBOR since it doesn't contradict SMILE or JSON, and its a last resort
            if (CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, bytes[offset])) {
                return true;
            }
            return false;
        }

        @Override
        public boolean detectedXContent(final CharSequence content, final int length) {
            return false;
        }

        @Override
        public XContentBuilder contentBuilder() throws IOException {
            return CborXContent.contentBuilder();
        }

        @Override
        public XContentBuilder contentBuilder(final OutputStream os) throws IOException {
            return new XContentBuilder(CborXContent.cborXContent, os);
        }
    };

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

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(this.mediaType());
    }
}
