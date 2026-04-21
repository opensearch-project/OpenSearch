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

package org.opensearch.common.xcontent.smile;

import org.opensearch.common.xcontent.XContentConstraints;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.XObjectReadContext;
import org.opensearch.common.xcontent.XObjectWriteContext;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentGenerator;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

import tools.jackson.core.JsonEncoding;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.StreamWriteConstraints;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.dataformat.smile.SmileFactory;
import tools.jackson.dataformat.smile.SmileFactoryBuilder;
import tools.jackson.dataformat.smile.SmileWriteFeature;

/**
 * A Smile based content implementation using Jackson.
 */
public class SmileXContent implements XContent, XContentConstraints {
    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(smileXContent);
    }

    static final SmileFactory smileFactory;
    public static final SmileXContent smileXContent;

    static {
        final SmileFactoryBuilder builder = new SmileFactoryBuilder(new SmileFactory());
        builder.configure(SmileFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        builder.streamWriteConstraints(StreamWriteConstraints.builder().maxNestingDepth(DEFAULT_MAX_DEPTH).build());
        builder.streamReadConstraints(
            StreamReadConstraints.builder()
                .maxStringLength(DEFAULT_MAX_STRING_LEN)
                .maxNameLength(DEFAULT_MAX_NAME_LEN)
                .maxNestingDepth(DEFAULT_MAX_DEPTH)
                .build()
        );
        // for now, this is an overhead, might make sense for web sockets
        builder.configure(SmileWriteFeature.ENCODE_BINARY_AS_7BIT, false);
        // Do not automatically close unclosed objects/arrays in tools.jackson.dataformat.smile.SmileGenerator#close() method
        builder.configure(StreamWriteFeature.AUTO_CLOSE_CONTENT, false);
        builder.configure(StreamReadFeature.STRICT_DUPLICATE_DETECTION, true);
        builder.configure(StreamReadFeature.USE_FAST_DOUBLE_PARSER, true);

        smileFactory = builder.build();
        smileXContent = new SmileXContent();
    }

    private SmileXContent() {}

    @Override
    public MediaType mediaType() {
        return XContentType.SMILE;
    }

    @Override
    public byte streamSeparator() {
        return (byte) 0xFF;
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes, boolean prettyPrint)
        throws IOException {
        return new SmileXContentGenerator(
            smileFactory.createGenerator(XObjectWriteContext.create(prettyPrint), os, JsonEncoding.UTF8),
            os,
            includes,
            excludes
        );
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, String content)
        throws IOException {
        return new SmileXContentParser(
            xContentRegistry,
            deprecationHandler,
            smileFactory.createParser(XObjectReadContext.create(), content)
        );
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, InputStream is)
        throws IOException {
        return new SmileXContentParser(xContentRegistry, deprecationHandler, smileFactory.createParser(XObjectReadContext.create(), is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, byte[] data)
        throws IOException {
        return new SmileXContentParser(xContentRegistry, deprecationHandler, smileFactory.createParser(XObjectReadContext.create(), data));
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        byte[] data,
        int offset,
        int length
    ) throws IOException {
        return new SmileXContentParser(
            xContentRegistry,
            deprecationHandler,
            smileFactory.createParser(XObjectReadContext.create(), data, offset, length)
        );
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, Reader reader)
        throws IOException {
        return new SmileXContentParser(
            xContentRegistry,
            deprecationHandler,
            smileFactory.createParser(XObjectReadContext.create(), reader)
        );
    }
}
