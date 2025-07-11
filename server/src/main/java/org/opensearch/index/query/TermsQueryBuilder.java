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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ConstantFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.indices.TermsLookup;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.logger;
// **NEW** Imports for sync fallback logic
import java.util.concurrent.CountDownLatch; // **NEW**
import java.util.concurrent.atomic.AtomicReference; // **NEW**
import org.opensearch.index.query.QueryRewriteContext; // Ensure this is present

/**
 * A filter for a field based on several terms matching on any of them.
 *
 * @opensearch.internal
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> implements WithFieldName {
    public static final String NAME = "terms";

    private final String fieldName;
    private final List<?> values;
    private final TermsLookup termsLookup;
    private final Supplier<List<?>> supplier;

    private static final ParseField VALUE_TYPE_FIELD = new ParseField("value_type");
    private ValueType valueType = ValueType.DEFAULT;
//    private final SetOnce<List<Object>> fetchedTerms = new SetOnce<>();
//    private final AtomicBoolean asyncRegistered = new AtomicBoolean(false);
    private static final Map<String, SetOnce<List<Object>>> fetchedTermsCache = new ConcurrentHashMap<>();
    private static final Map<String, AtomicBoolean> asyncRegistrationCache = new ConcurrentHashMap<>();



    /**
     * Terms query may accept different types of value
     * <p>
     * This flag is used to decide how to parse the value and build query upon later
     */
    public enum ValueType {
        DEFAULT("default"),
        BITMAP("bitmap");

        private final String type;

        ValueType(String type) {
            this.type = type;
        }

        public static ValueType fromString(String type) {
            for (ValueType valueType : ValueType.values()) {
                if (valueType.type.equalsIgnoreCase(type)) {
                    return valueType;
                }
            }
            throw new IllegalArgumentException(type + " is not valid " + VALUE_TYPE_FIELD);
        }
    }

    public TermsQueryBuilder valueType(ValueType valueType) {
        this.valueType = valueType;
        return this;
    }

    public TermsQueryBuilder(String fieldName, TermsLookup termsLookup) {
        this(fieldName, null, termsLookup);
    }

    /**
     * constructor used internally for serialization of both value / termslookup variants
     */
    TermsQueryBuilder(String fieldName, List<Object> values, TermsLookup termsLookup) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (values == null && termsLookup == null) {
            throw new IllegalArgumentException("No value or termsLookup specified for terms query");
        }
        if (values != null && termsLookup != null) {
            throw new IllegalArgumentException("Both values and termsLookup specified for terms query");
        }
        this.fieldName = fieldName;
        this.values = values == null ? null : convert(values);
        this.termsLookup = termsLookup;
        this.supplier = null;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, String... values) {
        this(fieldName, values != null ? Arrays.asList(values) : null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, int... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).collect(Collectors.toList()) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, long... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).collect(Collectors.toList()) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, float... values) {
        this(
            fieldName,
            values != null ? IntStream.range(0, values.length).mapToObj(i -> values[i]).collect(Collectors.toList()) : (Iterable<?>) null
        );
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, double... values) {
        this(fieldName, values != null ? Arrays.stream(values).mapToObj(s -> s).collect(Collectors.toList()) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Object... values) {
        this(fieldName, values != null ? Arrays.asList(values) : (Iterable<?>) null);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param fieldName The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String fieldName, Iterable<?> values) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name cannot be null.");
        }
        if (values == null) {
            throw new IllegalArgumentException("No value specified for terms query");
        }
        this.fieldName = fieldName;
        this.values = convert(values);
        this.termsLookup = null;
        this.supplier = null;
    }

    private TermsQueryBuilder(String fieldName, Iterable<?> values, ValueType valueType) {
        this(fieldName, values);
        this.valueType = valueType;
    }

    private TermsQueryBuilder(String fieldName, Supplier<List<?>> supplier) {
        this.fieldName = fieldName;
        this.values = null;
        this.termsLookup = null;
        this.supplier = supplier;
    }

    private TermsQueryBuilder(String fieldName, Supplier<List<?>> supplier, ValueType valueType) {
        this(fieldName, supplier);
        this.valueType = valueType;
    }

    /**
     * Read from a stream.
     */
    public TermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        termsLookup = in.readOptionalWriteable(TermsLookup::new);
        values = (List<?>) in.readGenericValue();
        this.supplier = null;
        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            valueType = in.readEnum(ValueType.class);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeOptionalWriteable(termsLookup);
        out.writeGenericValue(values);
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeEnum(valueType);
        }
    }

    @Override
    public String fieldName() {
        return this.fieldName;
    }

    public ValueType valueType() {
        return this.valueType;
    }

    public List<Object> values() {
        return convertBack(this.values);
    }

    public TermsLookup termsLookup() {
        return this.termsLookup;
    }

    private static final Set<Class<? extends Number>> INTEGER_TYPES = new HashSet<>(
        Arrays.asList(Byte.class, Short.class, Integer.class, Long.class)
    );
    private static final Set<Class<?>> STRING_TYPES = new HashSet<>(Arrays.asList(BytesRef.class, String.class));

    /**
     * Same as {@link #convert(List)} but on an {@link Iterable}.
     */
    private static List<?> convert(Iterable<?> values) {
        List<?> list;
        if (values instanceof List<?>) {
            list = (List<?>) values;
        } else {
            ArrayList<Object> arrayList = new ArrayList<>();
            for (Object o : values) {
                arrayList.add(o);
            }
            list = arrayList;
        }
        return convert(list);
    }

    /**
     * Convert the list in a way that optimizes storage in the case that all
     * elements are either integers or {@link String}s/{@link BytesRef}/
     * {@link CharBuffer}s. This is useful to help garbage collections for
     * use-cases that involve sending very large terms queries to OpenSearch.
     * If the list does not only contain integers or {@link String}s, then a
     * list is returned where all {@link String}/{@link CharBuffer}s have been
     * replaced with {@link BytesRef}s.
     */
    static List<?> convert(List<?> list) {
        if (list.isEmpty()) {
            return Collections.emptyList();
        }

        final boolean allNumbers = list.stream().allMatch(o -> o != null && INTEGER_TYPES.contains(o.getClass()));
        if (allNumbers) {
            final long[] elements = list.stream().mapToLong(o -> ((Number) o).longValue()).toArray();
            return new AbstractList<Object>() {
                @Override
                public Object get(int index) {
                    return elements[index];
                }

                @Override
                public int size() {
                    return elements.length;
                }
            };
        }

        final boolean allStrings = list.stream().allMatch(o -> o != null && STRING_TYPES.contains(o.getClass()));
        if (allStrings) {
            final BytesRefBuilder builder = new BytesRefBuilder();
            try (BytesStreamOutput bytesOut = new BytesStreamOutput()) {
                final int[] endOffsets = new int[list.size()];
                int i = 0;
                for (Object o : list) {
                    BytesRef b;
                    if (o instanceof BytesRef) {
                        b = (BytesRef) o;
                    } else if (o instanceof CharBuffer) {
                        b = new BytesRef((CharBuffer) o);
                    } else {
                        builder.copyChars(o.toString());
                        b = builder.get();
                    }
                    bytesOut.writeBytes(b.bytes, b.offset, b.length);
                    if (i == 0) {
                        endOffsets[0] = b.length;
                    } else {
                        endOffsets[i] = Math.addExact(endOffsets[i - 1], b.length);
                    }
                    ++i;
                }
                final BytesReference bytes = bytesOut.bytes();
                return new AbstractList<Object>() {
                    @Override
                    public Object get(int i) {
                        final int startOffset = i == 0 ? 0 : endOffsets[i - 1];
                        final int endOffset = endOffsets[i];
                        return bytes.slice(startOffset, endOffset - startOffset).toBytesRef();
                    }

                    @Override
                    public int size() {
                        return endOffsets.length;
                    }
                };
            }
        }

        return list.stream().map(o -> o instanceof String ? new BytesRef(o.toString()) : o).collect(Collectors.toList());
    }

    /**
     * Convert the internal {@link List} of values back to a user-friendly list.
     * Integers are kept as-is since the terms query does not make any difference
     * between {@link Integer}s and {@link Long}s, but {@link BytesRef}s are
     * converted back to {@link String}s.
     */
    static List<Object> convertBack(List<?> list) {
        return new AbstractList<Object>() {
            @Override
            public int size() {
                return list.size();
            }

            @Override
            public Object get(int index) {
                Object o = list.get(index);
                if (o instanceof BytesRef) {
                    o = ((BytesRef) o).utf8ToString();
                }
                // we do not convert longs, all integer types are equivalent
                // as far as this query is concerned
                return o;
            }
        };
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (this.termsLookup != null) {
            builder.startObject(fieldName);
            termsLookup.toXContent(builder, params);
            builder.endObject();
        } else {
            builder.field(fieldName, convertBack(values));
        }
        printBoostAndQueryName(builder);
        if (valueType != ValueType.DEFAULT) {
            builder.field(VALUE_TYPE_FIELD.getPreferredName(), valueType.type);
        }
        builder.endObject();
    }

    public static TermsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        List<Object> values = null;
        TermsLookup termsLookup = null;
        QueryBuilder nestedQuery = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String valueTypeStr = ValueType.DEFAULT.type;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (fieldName != null) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + TermsQueryBuilder.NAME + "] query does not support multiple fields"
                    );
                }
                fieldName = currentFieldName;
                values = parseValues(parser);
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "["
                            + TermsQueryBuilder.NAME
                            + "] query does not support more than one field. "
                            + "Already got: ["
                            + fieldName
                            + "] but also found ["
                            + currentFieldName
                            + "]"
                    );
                }
                fieldName = currentFieldName;
                termsLookup = TermsLookup.parseTermsLookup(parser);
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (VALUE_TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    valueTypeStr = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + TermsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + TermsQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        if (fieldName == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "["
                    + TermsQueryBuilder.NAME
                    + "] query requires a field name, "
                    + "followed by array of terms or a document lookup specification"
            );
        }

        ValueType valueType = ValueType.fromString(valueTypeStr);
        if (valueType == ValueType.BITMAP) {
            if (values != null && values.size() == 1 && values.get(0) instanceof BytesRef) {
                values.set(0, new BytesArray(Base64.getDecoder().decode(((BytesRef) values.get(0)).utf8ToString())));
            } else if (termsLookup == null) {
                throw new IllegalArgumentException(
                    "Invalid value for bitmap type: Expected a single-element array with a base64 encoded serialized bitmap."
                );
            }
        }

        return new TermsQueryBuilder(fieldName, values, termsLookup).boost(boost).queryName(queryName).valueType(valueType);
    }

    static List<Object> parseValues(XContentParser parser) throws IOException {
        List<Object> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            Object value = maybeConvertToBytesRef(parser.objectBytes());
            if (value == null) {
                throw new ParsingException(parser.getTokenLocation(), "No value specified for terms query");
            }
            values.add(value);
        }
        return values;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // All remote fetching for termsLookup must be completed in the rewrite phase.
        // doToQuery should only use cached results from rewrite, never perform I/O!
        if (termsLookup != null && termsLookup.query() != null) {
            String key = cacheKey();
            SetOnce<List<Object>> fetchedTerms = fetchedTermsCache.get(key);
            logger.info("[DO-TO-QUERY] Checking fetchedTerms presence for key: " + cacheKey());
            logger.info("[DO-TO-QUERY] fetchedTerms is " + (fetchedTerms == null ? "null" : (fetchedTerms.get() == null ? "not yet set" : "set with " + fetchedTerms.get().size() + " terms")));
            logger.info("In doToQuery for field " + fieldName + ", fetchedTerms is " +
                (fetchedTerms == null ? "null" : fetchedTerms.get() == null ? "not yet set" : "set with " + fetchedTerms.get().size() + " terms"));
            // This should NEVER be null here—rewrite must guarantee terms are fetched!
            if (fetchedTerms == null || fetchedTerms.get() == null) {
                throw new IllegalStateException("Terms must be fetched during rewrite phase before query execution.");
            }
            // Use the fetched terms, guaranteed by rewrite to be present
            return context.fieldMapper(fieldName).termsQuery(fetchedTerms.get(), context);
//            QueryBuilder rewrittenQuery = termsLookup.query().rewrite(context);
//
//            SearchResponse response = context.getClient()
//                .search(new SearchRequest(termsLookup.index()).source(new SearchSourceBuilder().query(rewrittenQuery).fetchSource(true)))
//                .actionGet();
//
//            List<Object> terms = new ArrayList<>();
//            for (SearchHit hit : response.getHits().getHits()) {
//                terms.addAll(XContentMapValues.extractRawValues(termsLookup.path(), hit.getSourceAsMap()));
//            }
//            return context.fieldMapper(fieldName).termsQuery(terms, context);
        }

        // This section ensures no on-demand fetching for other cases as well
        if (termsLookup != null || supplier != null || values == null || values.isEmpty()) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        int maxTermsCount = context.getIndexSettings().getMaxTermsCount();
        if (values.size() > maxTermsCount) {
            throw new IllegalArgumentException(
                "The number of terms ["
                    + values.size()
                    + "] used in the Terms Query request has exceeded "
                    + "the allowed maximum of ["
                    + maxTermsCount
                    + "]. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_TERMS_COUNT_SETTING.getKey()
                    + "] index level setting."
            );
        }
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }

        if (valueType == ValueType.BITMAP) {
            if (values.size() == 1 && values.get(0) instanceof BytesArray) {
                if (fieldType.unwrap() instanceof NumberFieldMapper.NumberFieldType) {
                    return ((NumberFieldMapper.NumberFieldType) fieldType).bitmapQuery((BytesArray) values.get(0));
                }
            }
        }
        return fieldType.termsQuery(values, context);
    }

    private void fetch(TermsLookup termsLookup, Client client, ActionListener<List<Object>> actionListener) {
        GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.id());
        getRequest.preference("_local").routing(termsLookup.routing());
        if (termsLookup.store()) {
            getRequest.storedFields(termsLookup.path());
        }
        client.get(getRequest, ActionListener.delegateFailure(actionListener, (delegatedListener, getResponse) -> {
            List<Object> terms = new ArrayList<>();
            if (termsLookup.store()) {
                List<Object> values = getResponse.getField(termsLookup.path()).getValues();
                if (values.size() != 1 && valueType == ValueType.BITMAP) {
                    throw new IllegalArgumentException(
                        "Invalid value for bitmap type: Expected a single base64 encoded serialized bitmap."
                    );
                }
                terms.addAll(values);
            } else {
                if (getResponse.isSourceEmpty() == false) { // extract terms only if the doc source exists
                    List<Object> extractedValues = XContentMapValues.extractRawValues(termsLookup.path(), getResponse.getSourceAsMap());
                    terms.addAll(extractedValues);
                }
            }
            delegatedListener.onResponse(terms);
        }));
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, termsLookup, supplier, valueType);
    }

    @Override
    protected boolean doEquals(TermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(values, other.values)
            && Objects.equals(termsLookup, other.termsLookup)
            && Objects.equals(supplier, other.supplier)
            && Objects.equals(valueType, other.valueType);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (termsLookup != null && termsLookup.query() != null) {
            // New code: rewrite the inner query first
            QueryBuilder innerQuery = termsLookup.query();
            QueryBuilder rewrittenInner = innerQuery.rewrite(queryRewriteContext);
            if (rewrittenInner != innerQuery) {
                TermsLookup rewrittenLookup = new TermsLookup(
                    termsLookup.index(),
                    null, // id is mutually exclusive with query
                    termsLookup.path(),
                    rewrittenInner
                ).routing(termsLookup.routing());

                return new TermsQueryBuilder(fieldName, rewrittenLookup);
            }

            QueryShardContext shardContext = queryRewriteContext.convertToShardContext();
            if (shardContext == null) {
                // NO CLIENT AVAILABLE! Cannot fetch terms via subquery lookup without shard context.
                // throw new IllegalStateException("Subquery-based terms lookup requires a shard context and cannot be performed on the coordinating node.");
                logger.info("[REWRITE] Shard Context is : " + shardContext);
                return this;
            }

            // Use a class-level cache for fetched terms keyed by the termsLookup params
            String key = cacheKey();
            logger.info("[REWRITE] Cache key: " + key);
            SetOnce<List<Object>> fetchedTerms = fetchedTermsCache.computeIfAbsent(key, k -> new SetOnce<>());
            logger.info("[REWRITE] fetchedTerms present? " + (fetchedTerms.get() != null));

            if (fetchedTerms.get() == null) {
                // Register async fetch only once
                AtomicBoolean asyncRegistered = asyncRegistrationCache.computeIfAbsent(key, k -> new AtomicBoolean(false));
                logger.info("[REWRITE] asyncRegistered current value: " + asyncRegistered.get());
                if (asyncRegistered.compareAndSet(false, true)) {
                    logger.info("[REWRITE] Registering async fetch for terms from lookup index");
                    queryRewriteContext.registerAsyncAction((client, listener) -> {
                        asyncFetchTerms(
                            shardContext,
                            client,
                            fetchedTerms,
                            ActionListener.wrap(
                                unused -> {
                                    logger.info("[ASYNC-CALLBACK] Async fetch completed successfully");
                                    listener.onResponse(null);
                                },
                                e -> {
                                    logger.error("[ASYNC-CALLBACK] Async fetch failed", e);
                                    listener.onFailure(e);
                                }
                            )
                        );
                    });
                } else {
                    logger.info("Async fetch already registered, skipping");
                }
                // Return this to trigger rewrite again after async fetch completes
                return this;
            }

            // Fetched terms are ready, build query directly using them
            logger.info("[REWRITE] Using fetchedTerms: " + fetchedTerms.get().size() + " terms for field: " + fieldName);
            return new TermsQueryBuilder(fieldName, fetchedTerms.get());
        }

//
//            // Fetched terms are ready, build query directly using them
//            logger.info("[REWRITE] Using fetchedTerms: " + fetchedTerms.get().size() + " terms for field: " + fieldName);
//            logger.info("Using fetchedTerms: " + fetchedTerms.get().size() + " terms for field: " + fieldName);
//            return new TermsQueryBuilder(fieldName, fetchedTerms.get());
//        }
//        if (termsLookup != null && termsLookup.query() != null) {
//            QueryShardContext shardContext = queryRewriteContext.convertToShardContext();
//            if (shardContext == null) {
//                return this;
//            }
//            // Rewrite the subquery using the shard context
//            QueryBuilder rewrittenQuery = termsLookup.query().rewrite(shardContext);
//
//            SearchResponse response;
//            try {
//                response = shardContext.getClient()
//                    .search(
//                        new SearchRequest(termsLookup.index()).source(new SearchSourceBuilder().query(rewrittenQuery).fetchSource(true))
//                    )
//                    .actionGet();
//            } catch (Exception e) {
//                throw new IllegalStateException("Failed to execute subquery: " + e.getMessage(), e);
//            }
//            // Extract terms from search hits
//            List<Object> terms = new ArrayList<>();
//            for (SearchHit hit : response.getHits().getHits()) {
//                Map<String, Object> source = hit.getSourceAsMap();
//                if (source != null) {
//                    try {
//                        List<Object> extracted = XContentMapValues.extractRawValues(termsLookup.path(), source);
//                        terms.addAll(extracted);
//                    } catch (Exception ex) {
//                        throw new IllegalStateException("Failed to execute subquery: " + ex.getMessage(), ex);
//                    }
//                } else {
//                    throw new IllegalStateException("Source is null for hit: " + hit);
//                }
//            }
//            // Return a new TermsQueryBuilder with the fetched terms
//            return new TermsQueryBuilder(fieldName, terms);
//        }

        if (supplier != null) {
            return supplier.get() == null ? this : new TermsQueryBuilder(this.fieldName, supplier.get(), valueType);
        } else if (this.termsLookup != null) {
            SetOnce<List<?>> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> fetch(termsLookup, client, ActionListener.map(listener, list -> {
                supplier.set(list);
                return null;
            })));
            return new TermsQueryBuilder(this.fieldName, supplier::get, valueType);
        }

        if (values == null || values.isEmpty()) {
            return new MatchNoneQueryBuilder();
        }

        QueryShardContext context = queryRewriteContext.convertToShardContext();
        if (context != null) {
            MappedFieldType fieldType = context.fieldMapper(this.fieldName);
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            } else if (fieldType.unwrap() instanceof ConstantFieldType) {
                // This logic is correct for all field types, but by only applying it to constant
                // fields we also have the guarantee that it doesn't perform I/O, which is important
                // since rewrites might happen on a network thread.
                Query query = fieldType.termsQuery(values, context);
                if (query instanceof MatchAllDocsQuery) {
                    return new MatchAllQueryBuilder();
                } else if (query instanceof MatchNoDocsQuery) {
                    return new MatchNoneQueryBuilder();
                } else {
                    assert false : "Constant fields must produce match-all or match-none queries, got " + query;
                }
            }
        }

        return this;
    }

    private void asyncFetchTerms(QueryShardContext shardContext, Client client, SetOnce<List<Object>> fetchedTerms, ActionListener<Void> listener) {
        logger.info("[ASYNC-FETCH] Starting async fetch for terms with key: " + cacheKey());
        logger.info("Starting async fetch for terms");
        logger.info("[ASYNC-FETCH] Starting async fetch for terms with key: " + cacheKey());
        if (shardContext == null) {
            logger.error("[ASYNC-FETCH] shardContext is null — cannot fetch");
            listener.onFailure(new IllegalStateException("Shard context is null"));
            return;
        }
        if (client == null) {
            logger.error("[ASYNC-FETCH] client is null — cannot fetch");
            listener.onFailure(new IllegalStateException("Client is null"));
            return;
        }
        try {
            logger.info("[ASYNC-FETCH] Rewriting subquery: " + termsLookup.query());
            QueryBuilder rewrittenQuery = termsLookup.query().rewrite(shardContext);
            logger.info("[ASYNC-FETCH] Rewritten subquery: " + rewrittenQuery);
            logger.info("Rewritten subquery: " + rewrittenQuery);

            SearchRequest searchRequest = new SearchRequest(termsLookup.index())
                .source(new SearchSourceBuilder()
                    .query(rewrittenQuery)
                    .fetchSource(true));
            logger.info("[ASYNC-FETCH] Sending async search request: " + searchRequest); // **NEW**

            client.search(searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    logger.info("[ASYNC-FETCH] Received response with hits: " + response.getHits().getHits().length);
                    logger.info("Async search response received");
                    try {
                        logger.info("[ASYNC-FETCH] Search response received with hits: " + response.getHits().getHits().length);
                        List<Object> terms = new ArrayList<>();
                        for (SearchHit hit : response.getHits().getHits()) {
                            logger.info("[ASYNC-FETCH] Processing hit with id: " + hit.getId());
                            Map<String, Object> source = hit.getSourceAsMap();
                            if (source != null) {

                                List<Object> extracted = XContentMapValues.extractRawValues(termsLookup.path(), source);
                                terms.addAll(extracted);
                            } else {
                                logger.error("Hit source is null for hit: " + hit);
                                listener.onFailure(new IllegalStateException("Source is null for hit: " + hit));
                                return;
                            }
                        }
                        logger.info("[ASYNC-FETCH] Extracted " + terms.size() + " total terms");
                        logger.info("[ASYNC-FETCH] fetchedTerms set successfully");
                        fetchedTerms.set(terms);
                        logger.info("Fetched terms set successfully: " + terms.size() + " terms");
                        logger.info("Calling listener.onResponse(null) with terms size: " + terms.size());
                        logger.info("[ASYNC-FETCH] Calling listener.onResponse(null) to signal async completion");
                        listener.onResponse(null);
                    } catch (Exception e) {
                        logger.error("[ASYNC-FETCH] fetchedTerms already set! Possible duplicate response", e);
                        logger.error("Exception during terms extraction", e);
                        listener.onFailure(new IllegalStateException("Failed to extract terms: " + e.getMessage(), e));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("[ASYNC-FETCH] Search request failed", e);
                    logger.error("[ASYNC-FETCH] Async fetch failed with exception: " + e.getMessage(), e);
                    logger.error("Async search failed", e);
                    listener.onFailure(new IllegalStateException("Failed to execute subquery: " + e.getMessage(), e));
                }
            });
            logger.info("[ASYNC-FETCH] Search request dispatched."); // **NEW**
        } catch (IOException e) {
            logger.error("[ASYNC-FETCH] Rewrite threw IOException: " + e.getMessage(), e);
            logger.error("Rewrite threw IOException", e);
            // Rewrite throws IOException, so handle here by failing listener
            listener.onFailure(e);
        }
    }

    private String cacheKey() {
        if (termsLookup == null) {
            return "";
        }
        // Compose a unique key based on index, path, routing, and query string
        String index = termsLookup.index();
        String path = termsLookup.path();
        String routing = termsLookup.routing() == null ? "" : termsLookup.routing();
        String queryString = termsLookup.query() == null ? "" : termsLookup.query().toString();

        return index + "|" + path + "|" + routing + "|" + queryString;
    }

}
