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
import org.opensearch.common.SetOnce;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
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
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A filter for a field based on several terms matching on any of them.
 *
 * @opensearch.internal
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> implements ComplementAwareQueryBuilder, WithFieldName {
    public static final String NAME = "terms";

    private final String fieldName;
    private final List<?> values;
    private final TermsLookup termsLookup;
    private final Supplier<List<?>> supplier;

    private static final ParseField VALUE_TYPE_FIELD = new ParseField("value_type");
    private ValueType valueType = ValueType.DEFAULT;

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
        if (values instanceof List<?> valuesList) {
            list = valuesList;
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
                    if (o instanceof BytesRef bytesRef) {
                        b = bytesRef;
                    } else if (o instanceof CharBuffer charBuffer) {
                        b = new BytesRef(charBuffer);
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

        return list.stream().map(o -> o instanceof String str ? new BytesRef(str) : o).collect(Collectors.toList());
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
                if (o instanceof BytesRef bytesRef) {
                    o = bytesRef.utf8ToString();
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
            if (values != null && values.size() == 1 && values.get(0) instanceof BytesRef bytesRef) {
                values.set(0, new BytesArray(Base64.getDecoder().decode(bytesRef.utf8ToString())));
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

        if (valueType == ValueType.BITMAP
            && values.size() == 1
            && values.get(0) instanceof BytesArray bytesArray
            && fieldType.unwrap() instanceof NumberFieldMapper.NumberFieldType numberFieldType) {
            return numberFieldType.bitmapQuery(bytesArray);
        }
        return fieldType.termsQuery(values, context);
    }

    private void fetch(TermsLookup termsLookup, Client client, ActionListener<List<Object>> actionListener) {
        if (termsLookup.id() != null) {
            GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.id());
            getRequest.preference("_local").routing(termsLookup.routing());
            if (termsLookup.store()) {
                getRequest.storedFields(termsLookup.path());
            } else {
                getRequest.fetchSourceContext(new FetchSourceContext(true, new String[] { termsLookup.path() }, null));
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
        } else if (termsLookup.query() != null) {
            client.admin()
                .indices()
                .getSettings(
                    new org.opensearch.action.admin.indices.settings.get.GetSettingsRequest().indices(termsLookup.index()),
                    ActionListener.wrap(settingsResponse -> {
                        // Get index-specific settings, fall back to defaults if missing
                        Settings idxSettings = settingsResponse.getIndexToSettings().getOrDefault(termsLookup.index(), Settings.EMPTY);

                        // Get max_terms_count, max_result_window, and max_clause_count, fallback to their defaults
                        int maxTermsCount = IndexSettings.MAX_TERMS_COUNT_SETTING.get(idxSettings);
                        int maxResultWindow = IndexSettings.MAX_RESULT_WINDOW_SETTING.get(idxSettings);
                        int maxClauseCount = idxSettings.getAsInt("indices.query.max_clause_count", 1024);
                        // The effective size must not exceed any of these
                        int fetchSize = Math.min(Math.min(maxTermsCount, maxResultWindow), maxClauseCount);

                        try {
                            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(termsLookup.query()).size(fetchSize);

                            // Use stored fields if possible, otherwise fetch source
                            if (termsLookup.store()) {
                                sourceBuilder.storedField(termsLookup.path());
                            } else {
                                sourceBuilder.fetchSource(termsLookup.path(), null);
                            }

                            SearchRequest searchRequest = new SearchRequest(termsLookup.index()).source(sourceBuilder);

                            client.search(
                                searchRequest,
                                ActionListener.delegateFailure(actionListener, (delegatedListener, searchResponse) -> {
                                    List<Object> terms = new ArrayList<>();
                                    SearchHit[] hits = searchResponse.getHits().getHits();
                                    long totalHits = searchResponse.getHits().getTotalHits().value();

                                    // Correctness: fail if total hits exceed fetchSize (results are incomplete)
                                    if (totalHits > fetchSize) {
                                        delegatedListener.onFailure(
                                            new IllegalArgumentException(
                                                "Terms lookup subquery total hits ["
                                                    + totalHits
                                                    + "] exceed fetch limit ["
                                                    + fetchSize
                                                    + "]; filter may be incomplete."
                                            )
                                        );
                                        return;
                                    }

                                    // Defensive: avoid exceeding maxTermsCount
                                    if (hits.length > maxTermsCount) {
                                        delegatedListener.onFailure(
                                            new IllegalArgumentException(
                                                "Terms lookup subquery result count ["
                                                    + hits.length
                                                    + "] exceeds allowed max_terms_count ["
                                                    + maxTermsCount
                                                    + "]"
                                            )
                                        );
                                        return;
                                    }

                                    for (SearchHit hit : hits) {
                                        if (termsLookup.store()) {
                                            if (hit.field(termsLookup.path()) != null) {
                                                terms.addAll(hit.field(termsLookup.path()).getValues());
                                            }
                                        } else {
                                            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                                            if (sourceAsMap != null) {
                                                List<Object> extractedValues = XContentMapValues.extractRawValues(
                                                    termsLookup.path(),
                                                    sourceAsMap
                                                );
                                                terms.addAll(extractedValues);
                                            }
                                        }
                                    }
                                    delegatedListener.onResponse(terms);
                                })
                            );

                        } catch (Exception e) {
                            actionListener.onFailure(e);
                        }
                    }, actionListener::onFailure)
                );
        } else {
            // No lookup type provided
            actionListener.onResponse(Collections.emptyList());
        }
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
        if (supplier != null) {
            return supplier.get() == null ? this : new TermsQueryBuilder(this.fieldName, supplier.get(), valueType);
        }
        // Support: terms lookup by document id && Support: terms lookup by subquery
        else if (this.termsLookup != null && (this.termsLookup.id() != null || this.termsLookup.query() != null)) {
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

    @Override
    public List<QueryBuilder> getComplement(QueryShardContext context) {
        // If this uses BITMAP value type, or if we're using termsLookup, we can't provide the complement.
        if (valueType.equals(ValueType.BITMAP)) return null;
        if (values == null || termsLookup != null) return null;
        // If this is a terms query on a numeric field, we can provide the complement using RangeQueryBuilder.
        NumberFieldMapper.NumberFieldType nft = ComplementHelperUtils.getNumberFieldType(context, fieldName);
        if (nft == null) return null;
        List<Number> numberValues = new ArrayList<>();
        for (Object value : values) {
            numberValues.add(nft.parse(value));
        }
        numberValues.sort(Comparator.comparingDouble(Number::doubleValue)); // For sorting purposes, use double value.
        NumberFieldMapper.NumberType numberType = nft.numberType();
        // If there is some other field type that's a whole number, this will still be correct, the complement may just have some
        // unnecessary components like "x < value < x + 1"
        boolean isWholeNumber = numberType.equals(NumberFieldMapper.NumberType.INTEGER)
            || numberType.equals(NumberFieldMapper.NumberType.LONG)
            || numberType.equals(NumberFieldMapper.NumberType.SHORT);
        return ComplementHelperUtils.numberValuesToComplement(fieldName, numberValues, isWholeNumber);
    }

}
