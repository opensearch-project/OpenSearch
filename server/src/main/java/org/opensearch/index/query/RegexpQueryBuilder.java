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

package org.opensearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that does fuzzy matching for a specific value.
 *
 * @opensearch.internal
 */
public class RegexpQueryBuilder extends AbstractQueryBuilder<RegexpQueryBuilder> implements MultiTermQueryBuilder {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RegexpQueryBuilder.class);

    public static final String NAME = "regexp";

    public static final int DEFAULT_FLAGS_VALUE = RegexpFlag.ALL.value();
    public static final int DEFAULT_DETERMINIZE_WORK_LIMIT = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;
    public static final boolean DEFAULT_CASE_INSENSITIVITY = false;

    private static final ParseField FLAGS_VALUE_FIELD = new ParseField("flags_value");
    private static final ParseField MAX_DETERMINIZED_STATES_FIELD = new ParseField("max_determinized_states");
    private static final ParseField FLAGS_FIELD = new ParseField("flags");
    private static final ParseField CASE_INSENSITIVE_FIELD = new ParseField("case_insensitive");
    private static final ParseField REWRITE_FIELD = new ParseField("rewrite");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    private final String fieldName;

    private final String value;

    private int syntaxFlagsValue = DEFAULT_FLAGS_VALUE;
    private boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;

    private int maxDeterminizedStates = DEFAULT_DETERMINIZE_WORK_LIMIT;

    private String rewrite;

    /**
     * Constructs a new regex query.
     *
     * @param fieldName  The name of the field
     * @param value The regular expression
     */
    public RegexpQueryBuilder(String fieldName, String value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public RegexpQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readString();
        syntaxFlagsValue = in.readVInt();
        maxDeterminizedStates = in.readVInt();
        rewrite = in.readOptionalString();
        caseInsensitive = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(value);
        out.writeVInt(syntaxFlagsValue);
        out.writeVInt(maxDeterminizedStates);
        out.writeOptionalString(rewrite);
        out.writeBoolean(caseInsensitive);
    }

    /** Returns the field name used in this query. */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /**
     *  Returns the value used in this query.
     */
    public String value() {
        return this.value;
    }

    public RegexpQueryBuilder flags(RegexpFlag... flags) {
        if (flags == null) {
            this.syntaxFlagsValue = DEFAULT_FLAGS_VALUE;
            return this;
        }
        int value = 0;
        if (flags.length == 0) {
            value = RegexpFlag.ALL.value;
        } else {
            for (RegexpFlag flag : flags) {
                value |= flag.value;
            }
        }
        this.syntaxFlagsValue = value;
        return this;
    }

    public RegexpQueryBuilder flags(int flags) {
        this.syntaxFlagsValue = flags;
        return this;
    }

    public int flags() {
        return this.syntaxFlagsValue;
    }

    public RegexpQueryBuilder caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }

    public boolean caseInsensitive() {
        return this.caseInsensitive;
    }

    /**
     * Sets the regexp maxDeterminizedStates.
     */
    public RegexpQueryBuilder maxDeterminizedStates(int value) {
        this.maxDeterminizedStates = value;
        return this;
    }

    public int maxDeterminizedStates() {
        return this.maxDeterminizedStates;
    }

    public RegexpQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public String rewrite() {
        return this.rewrite;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(VALUE_FIELD.getPreferredName(), this.value);
        builder.field(FLAGS_VALUE_FIELD.getPreferredName(), syntaxFlagsValue);
        if (caseInsensitive != DEFAULT_CASE_INSENSITIVITY) {
            builder.field(CASE_INSENSITIVE_FIELD.getPreferredName(), caseInsensitive);
        }
        builder.field(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), maxDeterminizedStates);
        if (rewrite != null) {
            builder.field(REWRITE_FIELD.getPreferredName(), rewrite);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static RegexpQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String rewrite = null;
        String value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int flagsValue = RegexpQueryBuilder.DEFAULT_FLAGS_VALUE;
        boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;
        int maxDeterminizedStates = RegexpQueryBuilder.DEFAULT_DETERMINIZE_WORK_LIMIT;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.textOrNull();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            rewrite = parser.textOrNull();
                        } else if (FLAGS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            String flags = parser.textOrNull();
                            flagsValue = RegexpFlag.resolveValue(flags);
                        } else if (MAX_DETERMINIZED_STATES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            maxDeterminizedStates = parser.intValue();
                        } else if (FLAGS_VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            flagsValue = parser.intValue();
                        } else if (CASE_INSENSITIVE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            caseInsensitive = parser.booleanValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[regexp] query does not support [" + currentFieldName + "]"
                            );
                        }
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = currentFieldName;
                value = parser.textOrNull();
            }
        }

        RegexpQueryBuilder result = new RegexpQueryBuilder(fieldName, value).flags(flagsValue)
            .maxDeterminizedStates(maxDeterminizedStates)
            .rewrite(rewrite)
            .boost(boost)
            .queryName(queryName);
        result.caseInsensitive(caseInsensitive);
        return result;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws QueryShardException, IOException {
        final int maxAllowedRegexLength = context.getIndexSettings().getMaxRegexLength();
        if (value.length() > maxAllowedRegexLength) {
            throw new IllegalArgumentException(
                "The length of regex ["
                    + value.length()
                    + "] used in the Regexp Query request has exceeded "
                    + "the allowed maximum of ["
                    + maxAllowedRegexLength
                    + "]. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey()
                    + "] index level setting."
            );
        }

        // Check if COMPLEMENT flag is being used
        // The COMPLEMENT flag maps to Lucene's DEPRECATED_COMPLEMENT which is marked for removal in Lucene 11
        // This deprecation warning helps users migrate their queries before the feature is completely removed
        if ((syntaxFlagsValue & RegexpFlag.COMPLEMENT.value()) != 0) {
            deprecationLogger.deprecate(
                "regexp_complement_operator",
                "The complement operator (~) for arbitrary patterns in regexp queries is deprecated and will be removed in a future version. "
                    + "Consider rewriting your query to use character class negation [^...] or other query types."
            );
        }

        MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(rewrite, null, LoggingDeprecationHandler.INSTANCE);

        int matchFlagsValue = caseInsensitive ? RegExp.ASCII_CASE_INSENSITIVE : 0;
        Query query = null;
        // For BWC we mask irrelevant bits (RegExp changed ALL from 0xffff to 0xff)
        // The hexadecimal for DEPRECATED_COMPLEMENT is 0x10000. The OR condition ensures COMPLEMENT ~ is preserved
        int sanitisedSyntaxFlag = syntaxFlagsValue & (RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT);

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            query = fieldType.regexpQuery(value, sanitisedSyntaxFlag, matchFlagsValue, maxDeterminizedStates, method, context);
        }
        if (query == null) {
            if (method == null) {
                method = MultiTermQuery.CONSTANT_SCORE_REWRITE;
            }
            query = new RegexpQuery(
                new Term(fieldName, BytesRefs.toBytesRef(value)),
                sanitisedSyntaxFlag,
                matchFlagsValue,
                RegexpQuery.DEFAULT_PROVIDER,
                maxDeterminizedStates,
                method
            );
        }
        return query;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, syntaxFlagsValue, caseInsensitive, maxDeterminizedStates, rewrite);
    }

    @Override
    protected boolean doEquals(RegexpQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(value, other.value)
            && Objects.equals(syntaxFlagsValue, other.syntaxFlagsValue)
            && Objects.equals(caseInsensitive, other.caseInsensitive)
            && Objects.equals(maxDeterminizedStates, other.maxDeterminizedStates)
            && Objects.equals(rewrite, other.rewrite);
    }
}
