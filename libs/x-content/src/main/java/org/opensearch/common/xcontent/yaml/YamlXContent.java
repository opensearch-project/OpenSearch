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

package org.opensearch.common.xcontent.yaml;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;
import java.util.regex.Pattern;

import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.nodes.Tag;
import org.snakeyaml.engine.v2.resolver.CoreScalarResolver;
import org.snakeyaml.engine.v2.resolver.ScalarResolver;
import org.snakeyaml.engine.v2.schema.CoreSchema;
import tools.jackson.core.JsonEncoding;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.StreamWriteConstraints;
import tools.jackson.core.io.IOContext;
import tools.jackson.dataformat.yaml.YAMLFactory;
import tools.jackson.dataformat.yaml.YAMLFactoryBuilder;
import tools.jackson.dataformat.yaml.YAMLParser;

/**
 * A YAML based content implementation using Jackson.
 */
public class YamlXContent implements XContent, XContentConstraints {
    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(yamlXContent);
    }

    static final YAMLFactory yamlFactory;
    public static final YamlXContent yamlXContent;

    static {
        final LoadSettings loadSettings = LoadSettings.builder()
            .setCodePointLimit(DEFAULT_CODEPOINT_LIMIT)
            .setBufferSize(DEFAULT_BUFFER_SIZE)
            .setSchema(new CoreSchema() {
                @Override
                public ScalarResolver getScalarResolver() {
                    return new CoreScalarResolver(true) {
                        public static final Pattern BOOL_EXT = Pattern.compile("^(?:on|On|ON|off|Off|OFF|yes|Yes|YES|no|No|NO)$");

                        @Override
                        protected void addImplicitResolvers() {
                            super.addImplicitResolvers();
                            addImplicitResolver(Tag.BOOL, BOOL_EXT, "oOyYnN");
                        }
                    };
                }
            })
            .build();
        final YAMLFactoryBuilder builder = new YAMLFactoryBuilder(new YAMLFactory()).loadSettings(loadSettings);
        builder.configure(StreamReadFeature.STRICT_DUPLICATE_DETECTION, true);
        builder.streamWriteConstraints(StreamWriteConstraints.builder().maxNestingDepth(DEFAULT_MAX_DEPTH).build());
        builder.streamReadConstraints(
            StreamReadConstraints.builder()
                .maxStringLength(DEFAULT_MAX_STRING_LEN)
                .maxNameLength(DEFAULT_MAX_NAME_LEN)
                .maxNestingDepth(DEFAULT_MAX_DEPTH)
                .build()
        );
        builder.configure(StreamReadFeature.USE_FAST_DOUBLE_PARSER, true);

        yamlFactory = new YAMLFactory(builder) {
            @Override
            protected YAMLParser _createParser(ObjectReadContext readCtxt, IOContext ioCtxt, Reader r) {
                return new YAMLParser(
                    readCtxt,
                    ioCtxt,
                    _getBufferRecycler(),
                    readCtxt.getStreamReadFeatures(_streamReadFeatures),
                    readCtxt.getFormatReadFeatures(_formatReadFeatures),
                    _loadSettings,
                    r
                ) {
                    @Override
                    protected Boolean _matchYAMLBoolean(String value, int len) {
                        switch (len) {
                            case 2:
                                if ("on".equalsIgnoreCase(value)) return Boolean.TRUE;
                                if ("no".equalsIgnoreCase(value)) return Boolean.FALSE;
                                break;
                            case 3:
                                if ("off".equalsIgnoreCase(value)) return Boolean.FALSE;
                                if ("yes".equalsIgnoreCase(value)) return Boolean.TRUE;
                                break;
                        }
                        return super._matchYAMLBoolean(value, len);
                    }
                };
            }

            @Override
            protected YAMLParser _createParser(ObjectReadContext readCtxt, IOContext ioCtxt, InputStream is) {
                return _createParser(readCtxt, ioCtxt, _createReader(is, null, ioCtxt));
            }
        };
        yamlXContent = new YamlXContent();
    }

    private YamlXContent() {}

    @Override
    public MediaType mediaType() {
        return XContentType.YAML;
    }

    @Override
    public byte streamSeparator() {
        throw new UnsupportedOperationException("yaml does not support stream parsing...");
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes, boolean prettyPrint)
        throws IOException {
        return new YamlXContentGenerator(
            yamlFactory.createGenerator(XObjectWriteContext.create(prettyPrint), os, JsonEncoding.UTF8),
            os,
            includes,
            excludes
        );
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, String content)
        throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(XObjectReadContext.create(), content));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, InputStream is)
        throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(XObjectReadContext.create(), is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, byte[] data)
        throws IOException {
        return new YamlXContentParser(
            xContentRegistry,
            deprecationHandler,
            // We use ByteArrayInputStream instead of byte[] for YAML because the parsing
            // will fail if the length of the byte[] array exceeds LoadSettings::bufferSize (which
            // we set to 8000 by default). We cannot predict the how large the buffer should be reliably.
            yamlFactory.createParser(XObjectReadContext.create(), new ByteArrayInputStream(data))
        );
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        byte[] data,
        int offset,
        int length
    ) throws IOException {
        return new YamlXContentParser(
            xContentRegistry,
            deprecationHandler,
            // We use ByteArrayInputStream instead of byte[] for YAML because the parsing
            // will fail if the length of the byte[] array exceeds LoadSettings::bufferSize (which
            // we set to 8000 by default). We cannot predict the how large the buffer should be reliably.
            yamlFactory.createParser(XObjectReadContext.create(), new ByteArrayInputStream(data, offset, length))
        );
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, Reader reader)
        throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(XObjectReadContext.create(), reader));
    }
}
