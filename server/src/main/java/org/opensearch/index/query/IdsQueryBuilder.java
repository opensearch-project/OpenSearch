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

import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.core.xcontent.ObjectParser.fromList;

/**
 * A query that will return only documents matching specific ids (and a type).
 *
 * @opensearch.internal
 */
public class IdsQueryBuilder extends AbstractQueryBuilder<IdsQueryBuilder> {
    public static final String NAME = "ids";
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IdsQueryBuilder.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Types are deprecated in [ids] queries.";

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField VALUES_FIELD = new ParseField("values");

    private final Set<String> ids = new HashSet<>();

    /**
     * Creates a new IdsQueryBuilder with no types specified upfront
     */
    public IdsQueryBuilder() {
        // nothing to do
    }

    /**
     * Read from a stream.
     */
    public IdsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            // types no longer relevant so ignore
            String[] types = in.readStringArray();
            if (types.length > 0) {
                throw new IllegalStateException("types are no longer supported in ids query but found [" + Arrays.toString(types) + "]");
            }
        }
        Collections.addAll(ids, in.readStringArray());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_2_0_0)) {
            // types not supported so send an empty array to previous versions
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        out.writeStringArray(ids.toArray(new String[0]));
    }

    /**
     * Adds ids to the query.
     */
    public IdsQueryBuilder addIds(String... ids) {
        if (ids == null) {
            throw new IllegalArgumentException("[" + NAME + "] ids cannot be null");
        }
        Collections.addAll(this.ids, ids);
        return this;
    }

    /**
     * Returns the ids for the query.
     */
    public Set<String> ids() {
        return this.ids;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(VALUES_FIELD.getPreferredName());
        for (String value : ids) {
            builder.value(value);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static final ObjectParser<IdsQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, IdsQueryBuilder::new);

    static {
        PARSER.declareStringArray(fromList(String.class, IdsQueryBuilder::addIds), IdsQueryBuilder.VALUES_FIELD);
        declareStandardFields(PARSER);
    }

    public static IdsQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (ids.isEmpty()) {
            return new MatchNoneQueryBuilder();
        }
        QueryShardContext context = queryRewriteContext.convertToShardContext();
        if (context != null && context.fieldMapper(IdFieldMapper.NAME) == null) {
            // no mappings yet
            return new MatchNoneQueryBuilder();
        }
        return super.doRewrite(queryRewriteContext);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType idField = context.getFieldType(IdFieldMapper.NAME);
        if (idField == null || ids.isEmpty()) {
            throw new IllegalStateException("Rewrite first");
        }
        return idField.termsQuery(new ArrayList<>(ids), context);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(ids);
    }

    @Override
    protected boolean doEquals(IdsQueryBuilder other) {
        return Objects.equals(ids, other.ids);
    }
}
