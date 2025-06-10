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

package org.opensearch.search.suggest.completion;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.CompletionFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.suggest.SuggestionBuilder;
import org.opensearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.opensearch.search.suggest.completion.context.ContextMapping;
import org.opensearch.search.suggest.completion.context.ContextMappings;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Defines a suggest command based on a prefix, typically to provide "auto-complete" functionality
 * for users as they type search terms. The implementation of the completion service uses FSTs that
 * are created at index-time and so must be defined in the mapping with the type "completion" before
 * indexing.
 *
 * @opensearch.internal
 */
public class CompletionSuggestionBuilder extends SuggestionBuilder<CompletionSuggestionBuilder> {

    private static final MediaType CONTEXT_BYTES_XCONTENT_TYPE = MediaTypeRegistry.JSON;

    static final ParseField CONTEXTS_FIELD = new ParseField("contexts", "context");
    static final ParseField SKIP_DUPLICATES_FIELD = new ParseField("skip_duplicates");

    public static final String SUGGESTION_NAME = "completion";

    /**
     * {
     *     "field" : STRING
     *     "size" : INT
     *     "fuzzy" : BOOLEAN | FUZZY_OBJECT
     *     "contexts" : QUERY_CONTEXTS
     *     "regex" : REGEX_OBJECT
     *     "payload" : STRING_ARRAY
     * }
     */
    private static final ObjectParser<CompletionSuggestionBuilder.InnerBuilder, Void> PARSER = new ObjectParser<>(SUGGESTION_NAME);
    static {
        PARSER.declareField((parser, completionSuggestionContext, context) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (parser.booleanValue()) {
                    completionSuggestionContext.fuzzyOptions = new FuzzyOptions.Builder().build();
                }
            } else {
                completionSuggestionContext.fuzzyOptions = FuzzyOptions.parse(parser);
            }
        }, FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        PARSER.declareField(
            (parser, completionSuggestionContext, context) -> completionSuggestionContext.regexOptions = RegexOptions.parse(parser),
            RegexOptions.REGEX_OPTIONS,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::field, FIELDNAME_FIELD);
        PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::analyzer, ANALYZER_FIELD);
        PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::size, SIZE_FIELD);
        PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::shardSize, SHARDSIZE_FIELD);
        PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(CONTEXT_BYTES_XCONTENT_TYPE);
            builder.copyCurrentStructure(p);
            v.contextBytes = BytesReference.bytes(builder);
            p.skipChildren();
        }, CONTEXTS_FIELD, ObjectParser.ValueType.OBJECT); // context is deprecated
        PARSER.declareBoolean(CompletionSuggestionBuilder::skipDuplicates, SKIP_DUPLICATES_FIELD);
    }

    protected FuzzyOptions fuzzyOptions;
    protected RegexOptions regexOptions;
    protected BytesReference contextBytes = null;
    protected boolean skipDuplicates = false;

    public CompletionSuggestionBuilder(String field) {
        super(field);
    }

    /**
     * internal copy constructor that copies over all class fields except for the field which is
     * set to the one provided in the first argument
     */
    private CompletionSuggestionBuilder(String fieldname, CompletionSuggestionBuilder in) {
        super(fieldname, in);
        fuzzyOptions = in.fuzzyOptions;
        regexOptions = in.regexOptions;
        contextBytes = in.contextBytes;
        skipDuplicates = in.skipDuplicates;
    }

    /**
     * Read from a stream.
     */
    public CompletionSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        fuzzyOptions = in.readOptionalWriteable(FuzzyOptions::new);
        regexOptions = in.readOptionalWriteable(RegexOptions::new);
        contextBytes = in.readOptionalBytesReference();
        skipDuplicates = in.readBoolean();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(fuzzyOptions);
        out.writeOptionalWriteable(regexOptions);
        out.writeOptionalBytesReference(contextBytes);
        out.writeBoolean(skipDuplicates);
    }

    /**
     * Sets the prefix to provide completions for.
     * The prefix gets analyzed by the suggest analyzer.
     */
    @Override
    public CompletionSuggestionBuilder prefix(String prefix) {
        super.prefix(prefix);
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with fuzziness of <code>fuzziness</code>
     */
    public CompletionSuggestionBuilder prefix(String prefix, Fuzziness fuzziness) {
        super.prefix(prefix);
        this.fuzzyOptions = new FuzzyOptions.Builder().setFuzziness(fuzziness).build();
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with full fuzzy options
     * see {@link FuzzyOptions.Builder}
     */
    public CompletionSuggestionBuilder prefix(String prefix, FuzzyOptions fuzzyOptions) {
        super.prefix(prefix);
        this.fuzzyOptions = fuzzyOptions;
        return this;
    }

    /**
     * Sets a regular expression pattern for prefixes to provide completions for.
     */
    @Override
    public CompletionSuggestionBuilder regex(String regex) {
        super.regex(regex);
        return this;
    }

    /**
     * Same as {@link #regex(String)} with full regular expression options
     * see {@link RegexOptions.Builder}
     */
    public CompletionSuggestionBuilder regex(String regex, RegexOptions regexOptions) {
        this.regex(regex);
        this.regexOptions = regexOptions;
        return this;
    }

    /**
     * Sets query contexts for completion
     * @param queryContexts named query contexts
     *                      see {@link org.opensearch.search.suggest.completion.context.CategoryQueryContext}
     *                      and {@link org.opensearch.search.suggest.completion.context.GeoQueryContext}
     */
    public CompletionSuggestionBuilder contexts(Map<String, List<? extends ToXContent>> queryContexts) {
        Objects.requireNonNull(queryContexts, "contexts must not be null");
        try {
            XContentBuilder contentBuilder = MediaTypeRegistry.contentBuilder(CONTEXT_BYTES_XCONTENT_TYPE);
            contentBuilder.startObject();
            for (Map.Entry<String, List<? extends ToXContent>> contextEntry : queryContexts.entrySet()) {
                contentBuilder.startArray(contextEntry.getKey());
                for (ToXContent queryContext : contextEntry.getValue()) {
                    queryContext.toXContent(contentBuilder, EMPTY_PARAMS);
                }
                contentBuilder.endArray();
            }
            contentBuilder.endObject();
            return contexts(contentBuilder);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private CompletionSuggestionBuilder contexts(XContentBuilder contextBuilder) {
        contextBytes = BytesReference.bytes(contextBuilder);
        return this;
    }

    /**
     * Returns whether duplicate suggestions should be filtered out.
     */
    public boolean skipDuplicates() {
        return skipDuplicates;
    }

    /**
     * Should duplicates be filtered or not. Defaults to {@code false}.
     */
    public CompletionSuggestionBuilder skipDuplicates(boolean skipDuplicates) {
        this.skipDuplicates = skipDuplicates;
        return this;
    }

    private static class InnerBuilder extends CompletionSuggestionBuilder {
        private String field;

        InnerBuilder() {
            super("_na_");
        }

        private InnerBuilder field(String field) {
            this.field = field;
            return this;
        }
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (fuzzyOptions != null) {
            fuzzyOptions.toXContent(builder, params);
        }
        if (regexOptions != null) {
            regexOptions.toXContent(builder, params);
        }
        if (skipDuplicates) {
            builder.field(SKIP_DUPLICATES_FIELD.getPreferredName(), skipDuplicates);
        }
        if (contextBytes != null) {
            try (InputStream stream = contextBytes.streamInput()) {
                builder.rawField(CONTEXTS_FIELD.getPreferredName(), stream);
            }
        }
        return builder;
    }

    public static CompletionSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
        CompletionSuggestionBuilder.InnerBuilder builder = new CompletionSuggestionBuilder.InnerBuilder();
        PARSER.parse(parser, builder, null);
        String field = builder.field;
        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (field == null) {
            throw new OpenSearchParseException("the required field option [" + FIELDNAME_FIELD.getPreferredName() + "] is missing");
        }
        return new CompletionSuggestionBuilder(field, builder);
    }

    @Override
    public SuggestionContext build(QueryShardContext context) throws IOException {
        CompletionSuggestionContext suggestionContext = new CompletionSuggestionContext(context);
        // copy over common settings to each suggestion builder
        final MapperService mapperService = context.getMapperService();
        populateCommonFields(mapperService, suggestionContext);
        suggestionContext.setSkipDuplicates(skipDuplicates);
        suggestionContext.setFuzzyOptions(fuzzyOptions);
        suggestionContext.setRegexOptions(regexOptions);
        if (shardSize != null) {
            suggestionContext.setShardSize(shardSize);
        }
        MappedFieldType mappedFieldType = mapperService.fieldType(suggestionContext.getField());
        if (mappedFieldType == null || mappedFieldType.unwrap() instanceof CompletionFieldMapper.CompletionFieldType == false) {
            throw new IllegalArgumentException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        if (mappedFieldType.unwrap() instanceof CompletionFieldMapper.CompletionFieldType) {
            CompletionFieldMapper.CompletionFieldType type = (CompletionFieldMapper.CompletionFieldType) mappedFieldType;
            suggestionContext.setFieldType(type);
            if (type.hasContextMappings() && contextBytes != null) {
                Map<String, List<ContextMapping.InternalQueryContext>> queryContexts = parseContextBytes(
                    contextBytes,
                    context.getXContentRegistry(),
                    type.getContextMappings()
                );
                suggestionContext.setQueryContexts(queryContexts);
            } else if (contextBytes != null) {
                throw new IllegalArgumentException("suggester [" + type.name() + "] doesn't expect any context");
            }
        }
        assert suggestionContext.getFieldType() != null : "no completion field type set";
        return suggestionContext;
    }

    static Map<String, List<ContextMapping.InternalQueryContext>> parseContextBytes(
        BytesReference contextBytes,
        NamedXContentRegistry xContentRegistry,
        ContextMappings contextMappings
    ) throws IOException {
        try (
            XContentParser contextParser = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                contextBytes,
                CONTEXT_BYTES_XCONTENT_TYPE
            )
        ) {
            contextParser.nextToken();
            Map<String, List<ContextMapping.InternalQueryContext>> queryContexts = new HashMap<>(contextMappings.size());
            assert contextParser.currentToken() == XContentParser.Token.START_OBJECT;
            XContentParser.Token currentToken;
            String currentFieldName;
            while ((currentToken = contextParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = contextParser.currentName();
                    final ContextMapping<?> mapping = contextMappings.get(currentFieldName);
                    queryContexts.put(currentFieldName, mapping.parseQueryContext(contextParser));
                }
            }
            return queryContexts;
        }
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    protected boolean doEquals(CompletionSuggestionBuilder other) {
        return skipDuplicates == other.skipDuplicates
            && Objects.equals(fuzzyOptions, other.fuzzyOptions)
            && Objects.equals(regexOptions, other.regexOptions)
            && Objects.equals(contextBytes, other.contextBytes);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fuzzyOptions, regexOptions, contextBytes, skipDuplicates);
    }
}
