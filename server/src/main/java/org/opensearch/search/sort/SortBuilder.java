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

package org.opensearch.search.sort;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedObjectNotFoundException;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Base class for sort object builders
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class SortBuilder<T extends SortBuilder<T>> implements NamedWriteable, ToXContentObject, Rewriteable<SortBuilder<?>> {

    protected SortOrder order = SortOrder.ASC;

    // parse fields common to more than one SortBuilder
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField NESTED_FILTER_FIELD = new ParseField("nested_filter");
    public static final ParseField NESTED_PATH_FIELD = new ParseField("nested_path");

    /**
     * Create a {@linkplain SortFieldAndFormat} from this builder.
     */
    protected abstract SortFieldAndFormat build(QueryShardContext context) throws IOException;

    /**
     * Create a {@linkplain BucketedSort} which is useful for sorting inside of aggregations.
     */
    public abstract BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra)
        throws IOException;

    /**
     * Set the order of sorting.
     */
    @SuppressWarnings("unchecked")
    public T order(SortOrder order) {
        Objects.requireNonNull(order, "sort order cannot be null.");
        this.order = order;
        return (T) this;
    }

    /**
     * Return the {@link SortOrder} used for this {@link SortBuilder}.
     */
    public SortOrder order() {
        return this.order;
    }

    public static List<SortBuilder<?>> fromXContent(XContentParser parser) throws IOException {
        List<SortBuilder<?>> sortFields = new ArrayList<>(2);
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    parseCompoundSortField(parser, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    String fieldName = parser.text();
                    sortFields.add(fieldOrScoreSort(fieldName));
                } else {
                    throw new IllegalArgumentException(
                        "malformed sort format, " + "within the sort array, an object, or an actual string are allowed"
                    );
                }
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            String fieldName = parser.text();
            sortFields.add(fieldOrScoreSort(fieldName));
        } else if (token == XContentParser.Token.START_OBJECT) {
            parseCompoundSortField(parser, sortFields);
        } else {
            throw new IllegalArgumentException("malformed sort format, either start with array, object, or an actual string");
        }
        return sortFields;
    }

    private static SortBuilder<?> fieldOrScoreSort(String fieldName) {
        if (fieldName.equals(ScoreSortBuilder.NAME)) {
            return new ScoreSortBuilder();
        } else {
            return new FieldSortBuilder(fieldName);
        }
    }

    private static void parseCompoundSortField(XContentParser parser, List<SortBuilder<?>> sortFields) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    SortOrder order = SortOrder.fromString(parser.text());
                    sortFields.add(fieldOrScoreSort(fieldName).order(order));
                } else {
                    try {
                        SortBuilder<?> sort = parser.namedObject(SortBuilder.class, fieldName, null);
                        sortFields.add(sort);
                    } catch (NamedObjectNotFoundException err) {
                        sortFields.add(FieldSortBuilder.fromXContent(parser, fieldName));
                    }
                }
            }
        }
    }

    public static Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sortBuilders, QueryShardContext context) throws IOException {
        List<SortField> sortFields = new ArrayList<>(sortBuilders.size());
        List<DocValueFormat> sortFormats = new ArrayList<>(sortBuilders.size());
        for (SortBuilder<?> builder : sortBuilders) {
            SortFieldAndFormat sf = builder.build(context);
            sortFields.add(sf.field);
            sortFormats.add(sf.format);
        }
        if (!sortFields.isEmpty()) {
            // optimize if we just sort on score non reversed, we don't really
            // need sorting
            boolean sort;
            if (sortFields.size() > 1) {
                sort = true;
            } else {
                SortField sortField = sortFields.get(0);
                if (sortField.getType() == SortField.Type.SCORE && !sortField.getReverse()) {
                    sort = false;
                } else {
                    sort = true;
                }
            }
            if (sort) {
                return Optional.of(
                    new SortAndFormats(new Sort(sortFields.toArray(new SortField[0])), sortFormats.toArray(new DocValueFormat[0]))
                );
            }
        }
        return Optional.empty();
    }

    protected static Nested resolveNested(QueryShardContext context, String nestedPath, QueryBuilder nestedFilter) throws IOException {
        NestedSortBuilder nestedSortBuilder = new NestedSortBuilder(nestedPath);
        nestedSortBuilder.setFilter(nestedFilter);
        return resolveNested(context, nestedSortBuilder);
    }

    protected static Nested resolveNested(QueryShardContext context, NestedSortBuilder nestedSort) throws IOException {
        final Query childQuery = resolveNestedQuery(context, nestedSort, null);
        if (childQuery == null) {
            return null;
        }
        final ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        final Query parentQuery;
        if (objectMapper == null) {
            parentQuery = Queries.newNonNestedFilter();
        } else {
            parentQuery = objectMapper.nestedTypeFilter();
        }
        return new Nested(context.bitsetFilter(parentQuery), childQuery, nestedSort, context.searcher());
    }

    private static Query resolveNestedQuery(QueryShardContext context, NestedSortBuilder nestedSort, Query parentQuery) throws IOException {
        if (nestedSort == null || nestedSort.getPath() == null) {
            return null;
        }

        String nestedPath = nestedSort.getPath();
        QueryBuilder nestedFilter = nestedSort.getFilter();
        NestedSortBuilder nestedNestedSort = nestedSort.getNestedSort();

        // verify our nested path
        ObjectMapper nestedObjectMapper = context.getObjectMapper(nestedPath);

        if (nestedObjectMapper == null) {
            throw new QueryShardException(context, "[nested] failed to find nested object under path [" + nestedPath + "]");
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new QueryShardException(context, "[nested] nested object under path [" + nestedPath + "] is not of nested type");
        }
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();

        // get our child query, potentially applying a users filter
        Query childQuery;
        try {
            context.nestedScope().nextLevel(nestedObjectMapper);
            if (nestedFilter != null) {
                assert nestedFilter == Rewriteable.rewrite(nestedFilter, context) : "nested filter is not rewritten";
                if (parentQuery == null) {
                    // this is for back-compat, original single level nested sorting never applied a nested type filter
                    childQuery = nestedFilter.toQuery(context);
                } else {
                    childQuery = Queries.filtered(nestedObjectMapper.nestedTypeFilter(), nestedFilter.toQuery(context));
                }
            } else {
                childQuery = nestedObjectMapper.nestedTypeFilter();
            }
        } finally {
            context.nestedScope().previousLevel();
        }

        // apply filters from the previous nested level
        if (parentQuery != null) {
            if (objectMapper != null) {
                childQuery = Queries.filtered(
                    childQuery,
                    new ToChildBlockJoinQuery(parentQuery, context.bitsetFilter(objectMapper.nestedTypeFilter()))
                );
            }
        }

        // wrap up our parent and child and either process the next level of nesting or return
        if (nestedNestedSort != null) {
            try {
                context.nestedScope().nextLevel(nestedObjectMapper);
                return resolveNestedQuery(context, nestedNestedSort, childQuery);
            } finally {
                context.nestedScope().previousLevel();
            }
        } else {
            return childQuery;
        }
    }

    protected static QueryBuilder parseNestedFilter(XContentParser parser) {
        try {
            return parseInnerQueryBuilder(parser);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), "Expected " + NESTED_FILTER_FIELD.getPreferredName() + " element.", e);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
