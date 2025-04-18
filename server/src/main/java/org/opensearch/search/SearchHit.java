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

package org.opensearch.search;

import org.apache.lucene.search.Explanation;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ObjectParser.ValueType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.lucene.Lucene.readExplanation;
import static org.opensearch.common.lucene.Lucene.writeExplanation;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureFieldName;

/**
 * A single search hit.
 *
 * @see SearchHits
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SearchHit implements Writeable, ToXContentObject, Iterable<DocumentField> {

    private final transient int docId;

    private static final float DEFAULT_SCORE = Float.NaN;
    private float score = DEFAULT_SCORE;

    private final Text id;

    private final NestedIdentity nestedIdentity;

    private long version = -1;
    private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

    private BytesReference source;

    private Map<String, DocumentField> documentFields;
    private final Map<String, DocumentField> metaFields;

    private Map<String, HighlightField> highlightFields = null;

    private SearchSortValues sortValues = SearchSortValues.EMPTY;

    private Map<String, Float> matchedQueries = new HashMap<>();

    private Explanation explanation;

    @Nullable
    private SearchShardTarget shard;

    // These two fields normally get set when setting the shard target, so they hold the same values as the target thus don't get
    // serialized over the wire. When parsing hits back from xcontent though, in most of the cases (whenever explanation is disabled)
    // we can't rebuild the shard target object so we need to set these manually for users retrieval.
    private transient String index;
    private transient String clusterAlias;

    private Map<String, Object> sourceAsMap;

    private Map<String, SearchHits> innerHits;

    // used only in tests
    public SearchHit(int docId) {
        this(docId, null, null, null);
    }

    public SearchHit(int docId, String id, Map<String, DocumentField> documentFields, Map<String, DocumentField> metaFields) {
        this(docId, id, null, documentFields, metaFields);
    }

    public SearchHit(
        int nestedTopDocId,
        String id,
        NestedIdentity nestedIdentity,
        Map<String, DocumentField> documentFields,
        Map<String, DocumentField> metaFields
    ) {
        this.docId = nestedTopDocId;
        if (id != null) {
            this.id = new Text(id);
        } else {
            this.id = null;
        }
        this.nestedIdentity = nestedIdentity;
        this.documentFields = documentFields == null ? emptyMap() : documentFields;
        this.metaFields = metaFields == null ? emptyMap() : metaFields;
    }

    public SearchHit(StreamInput in) throws IOException {
        docId = -1;
        score = in.readFloat();
        id = in.readOptionalText();
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readOptionalText();
        }
        nestedIdentity = in.readOptionalWriteable(NestedIdentity::new);
        version = in.readLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        source = in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        documentFields = in.readMap(StreamInput::readString, DocumentField::new);
        metaFields = in.readMap(StreamInput::readString, DocumentField::new);

        int size = in.readVInt();
        if (size == 0) {
            highlightFields = emptyMap();
        } else if (size == 1) {
            HighlightField field = new HighlightField(in);
            highlightFields = singletonMap(field.name(), field);
        } else {
            Map<String, HighlightField> highlightFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = new HighlightField(in);
                highlightFields.put(field.name(), field);
            }
            this.highlightFields = unmodifiableMap(highlightFields);
        }

        sortValues = new SearchSortValues(in);

        size = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            if (size > 0) {
                Map<String, Float> tempMap = in.readMap(StreamInput::readString, StreamInput::readFloat);
                matchedQueries = tempMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                    );
            }
        } else {
            matchedQueries = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                matchedQueries.put(in.readString(), Float.NaN);
            }
        }
        // we call the setter here because that also sets the local index parameter
        shard(in.readOptionalWriteable(SearchShardTarget::new));
        size = in.readVInt();
        if (size > 0) {
            innerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                SearchHits value = new SearchHits(in);
                innerHits.put(key, value);
            }
        } else {
            innerHits = null;
        }
    }

    private static final Text SINGLE_MAPPING_TYPE = new Text(MapperService.SINGLE_MAPPING_NAME);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(score);
        out.writeOptionalText(id);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeOptionalText(SINGLE_MAPPING_TYPE);
        }
        out.writeOptionalWriteable(nestedIdentity);
        out.writeLong(version);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeBytesReference(source);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        out.writeMap(documentFields, StreamOutput::writeString, (stream, documentField) -> documentField.writeTo(stream));
        out.writeMap(metaFields, StreamOutput::writeString, (stream, documentField) -> documentField.writeTo(stream));
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(highlightFields.size());
            for (HighlightField highlightField : highlightFields.values()) {
                highlightField.writeTo(out);
            }
        }
        sortValues.writeTo(out);

        out.writeVInt(matchedQueries.size());
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            if (!matchedQueries.isEmpty()) {
                out.writeMap(matchedQueries, StreamOutput::writeString, StreamOutput::writeFloat);
            }
        } else {
            for (String matchedFilter : matchedQueries.keySet()) {
                out.writeString(matchedFilter);
            }
        }
        out.writeOptionalWriteable(shard);
        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(innerHits.size());
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    public int docId() {
        return this.docId;
    }

    public void score(float score) {
        this.score = score;
    }

    /**
     * The score.
     */
    public float getScore() {
        return this.score;
    }

    public void version(long version) {
        this.version = version;
    }

    /**
     * The version of the hit.
     */
    public long getVersion() {
        return this.version;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    /**
     * returns the sequence number of the last modification to the document, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * if not requested.
     **/
    public long getSeqNo() {
        return this.seqNo;
    }

    /**
     * returns the primary term of the last modification to the document, or {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM}
     * if not requested. */
    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    /**
     * The index of the hit.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id != null ? id.string() : null;
    }

    /**
     * If this is a nested hit then nested reference information is returned otherwise <code>null</code> is returned.
     */
    public NestedIdentity getNestedIdentity() {
        return nestedIdentity;
    }

    /**
     * Returns bytes reference, also uncompress the source if needed.
     */
    public BytesReference getSourceRef() {
        if (this.source == null) {
            return null;
        }

        try {
            this.source = CompressorRegistry.uncompressIfNeeded(this.source);
            return this.source;
        } catch (IOException e) {
            throw new OpenSearchParseException("failed to decompress source", e);
        }
    }

    /**
     * Sets representation, might be compressed....
     */
    public SearchHit sourceRef(BytesReference source) {
        this.source = source;
        this.sourceAsMap = null;
        return this;
    }

    /**
     * Is the source available or not. A source with no fields will return true. This will return false if {@code fields} doesn't contain
     * {@code _source} or if source is disabled in the mapping.
     */
    public boolean hasSource() {
        return source != null;
    }

    /**
     * The source of the document as string (can be {@code null}).
     */
    public String getSourceAsString() {
        if (source == null) {
            return null;
        }
        try {
            return XContentHelper.convertToJson(getSourceRef(), false);
        } catch (IOException e) {
            throw new OpenSearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document as a map (can be {@code null}).
     */
    public Map<String, Object> getSourceAsMap() {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    @Override
    public Iterator<DocumentField> iterator() {
        // need to join the fields and metadata fields
        Map<String, DocumentField> allFields = this.getFields();
        return allFields.values().iterator();
    }

    /**
     * The hit field matching the given field name.
     */
    public DocumentField field(String fieldName) {
        DocumentField result = documentFields.get(fieldName);
        if (result != null) {
            return result;
        } else {
            return metaFields.get(fieldName);
        }
    }

    /*
     * Adds a new DocumentField to the map in case both parameters are not null.
     * */
    public void setDocumentField(String fieldName, DocumentField field) {
        if (fieldName == null || field == null) return;
        if (documentFields.isEmpty()) this.documentFields = new HashMap<>();
        this.documentFields.put(fieldName, field);
    }

    public DocumentField removeDocumentField(String fieldName) {
        return documentFields.remove(fieldName);
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    public Map<String, DocumentField> getFields() {
        if (!metaFields.isEmpty() || !documentFields.isEmpty()) {
            final Map<String, DocumentField> fields = new HashMap<>();
            fields.putAll(metaFields);
            fields.putAll(documentFields);
            return fields;
        } else {
            return emptyMap();
        }
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    public Map<String, DocumentField> getMetaFields() {
        if (!metaFields.isEmpty()) {
            final Map<String, DocumentField> fields = new HashMap<>();
            fields.putAll(metaFields);
            return fields;
        } else {
            return emptyMap();
        }
    }

    /**
     * A map of hit fields (from field name to hit fields) if additional fields
     * were required to be loaded.
     */
    public Map<String, DocumentField> getDocumentFields() {
        if (!documentFields.isEmpty()) {
            final Map<String, DocumentField> fields = new HashMap<>();
            fields.putAll(documentFields);
            return fields;
        } else {
            return emptyMap();
        }
    }

    /**
     * A map of highlighted fields.
     */
    public Map<String, HighlightField> getHighlightFields() {
        return highlightFields == null ? emptyMap() : highlightFields;
    }

    public void highlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public void sortValues(Object[] sortValues, DocValueFormat[] sortValueFormats) {
        sortValues(new SearchSortValues(sortValues, sortValueFormats));
    }

    public void sortValues(SearchSortValues sortValues) {
        this.sortValues = sortValues;
    }

    /**
     * An array of the (formatted) sort values used.
     */
    public Object[] getSortValues() {
        return sortValues.getFormattedSortValues();
    }

    /**
     * An array of the (raw) sort values used.
     */
    public Object[] getRawSortValues() {
        return sortValues.getRawSortValues();
    }

    /**
     * If enabled, the explanation of the search hit.
     */
    public Explanation getExplanation() {
        return explanation;
    }

    public void explanation(Explanation explanation) {
        this.explanation = explanation;
    }

    /**
     * The shard of the search hit.
     */
    public SearchShardTarget getShard() {
        return shard;
    }

    public void shard(SearchShardTarget target) {
        if (innerHits != null) {
            for (SearchHits innerHits : innerHits.values()) {
                for (SearchHit innerHit : innerHits) {
                    innerHit.shard(target);
                }
            }
        }

        this.shard = target;
        if (target != null) {
            this.index = target.getIndex();
            this.clusterAlias = target.getClusterAlias();
        }
    }

    /**
     * Returns the cluster alias this hit comes from or null if it comes from a local cluster
     */
    public String getClusterAlias() {
        return clusterAlias;
    }

    public void matchedQueries(String[] matchedQueries) {
        if (matchedQueries != null) {
            for (String query : matchedQueries) {
                this.matchedQueries.put(query, Float.NaN);
            }
        }
    }

    public void matchedQueriesWithScores(Map<String, Float> matchedQueries) {
        if (matchedQueries != null) {
            this.matchedQueries = matchedQueries;
        }
    }

    /**
     * The set of query and filter names the query matched with. Mainly makes sense for compound filters and queries.
     */
    public String[] getMatchedQueries() {
        return matchedQueries == null ? new String[0] : matchedQueries.keySet().toArray(new String[0]);
    }

    /**
     * Returns the score of the provided named query if it matches.
     * <p>
     * If the 'include_named_queries_score' is not set, this method will return {@link Float#NaN}
     * for each named query instead of a numerical score.
     * </p>
     *
     * @param name The name of the query to retrieve the score for.
     * @return The score of the named query, or {@link Float#NaN} if 'include_named_queries_score' is not set.
     */
    public Float getMatchedQueryScore(String name) {
        return getMatchedQueriesAndScores().get(name);
    }

    /**
     * @return The map of the named queries that matched and their associated score.
     */
    public Map<String, Float> getMatchedQueriesAndScores() {
        return matchedQueries == null ? Collections.emptyMap() : matchedQueries;
    }

    /**
     * @return Inner hits or <code>null</code> if there are none
     */
    public Map<String, SearchHits> getInnerHits() {
        return innerHits;
    }

    public void setInnerHits(Map<String, SearchHits> innerHits) {
        this.innerHits = innerHits;
    }

    /**
     * Fields in a search hit used for parsing and toXContent
     *
     * @opensearch.internal
     */
    public static class Fields {
        static final String _INDEX = "_index";
        static final String _ID = "_id";
        static final String _VERSION = "_version";
        static final String _SEQ_NO = "_seq_no";
        static final String _PRIMARY_TERM = "_primary_term";
        static final String _SCORE = "_score";
        static final String FIELDS = "fields";
        static final String HIGHLIGHT = "highlight";
        static final String SORT = "sort";
        static final String MATCHED_QUERIES = "matched_queries";
        static final String _EXPLANATION = "_explanation";
        static final String VALUE = "value";
        static final String DESCRIPTION = "description";
        static final String DETAILS = "details";
        static final String INNER_HITS = "inner_hits";
        static final String _SHARD = "_shard";
        static final String _NODE = "_node";
    }

    // Following are the keys for storing the metadata fields and regular fields in the aggregation map.
    // These do not influence the structure of json serialization: document fields are still stored
    // under FIELDS and metadata are still scattered at the root level.
    static final String DOCUMENT_FIELDS = "document_fields";
    static final String METADATA_FIELDS = "metadata_fields";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toInnerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    // public because we render hit as part of completion suggestion option
    public XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (getExplanation() != null && shard != null) {
            builder.field(Fields._SHARD, shard.getShardId());
            builder.field(Fields._NODE, shard.getNodeIdText());
        }
        if (index != null) {
            builder.field(Fields._INDEX, RemoteClusterAware.buildRemoteIndexName(clusterAlias, index));
        }
        if (id != null) {
            builder.field(Fields._ID, id);
        }
        if (nestedIdentity != null) {
            nestedIdentity.toXContent(builder, params);
        }
        if (version != -1) {
            builder.field(Fields._VERSION, version);
        }

        if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            builder.field(Fields._SEQ_NO, seqNo);
            builder.field(Fields._PRIMARY_TERM, primaryTerm);
        }

        if (Float.isNaN(score)) {
            builder.nullField(Fields._SCORE);
        } else {
            builder.field(Fields._SCORE, score);
        }

        for (DocumentField field : metaFields.values()) {
            // ignore empty metadata fields
            if (field.getValues().isEmpty()) {
                continue;
            }
            // _ignored is the only multi-valued meta field
            // TODO: can we avoid having an exception here?
            if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                builder.field(field.getName(), field.getValues());
            } else {
                builder.field(field.getName(), field.<Object>getValue());
            }
        }
        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }
        if (documentFields.isEmpty() == false &&
        // ignore fields all together if they are all empty
            documentFields.values().stream().anyMatch(df -> !df.getValues().isEmpty())) {
            builder.startObject(Fields.FIELDS);
            for (DocumentField field : documentFields.values()) {
                if (!field.getValues().isEmpty()) {
                    field.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        if (highlightFields != null && !highlightFields.isEmpty()) {
            builder.startObject(Fields.HIGHLIGHT);
            for (HighlightField field : highlightFields.values()) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        sortValues.toXContent(builder, params);
        if (!matchedQueries.isEmpty()) {
            boolean includeMatchedQueriesScore = params.paramAsBoolean(RestSearchAction.INCLUDE_NAMED_QUERIES_SCORE_PARAM, false);
            if (includeMatchedQueriesScore) {
                builder.startObject(Fields.MATCHED_QUERIES);
                for (Map.Entry<String, Float> entry : matchedQueries.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            } else {
                builder.startArray(Fields.MATCHED_QUERIES);
                for (String matchedFilter : matchedQueries.keySet()) {
                    builder.value(matchedFilter);
                }
                builder.endArray();
            }
        }
        if (getExplanation() != null) {
            builder.field(Fields._EXPLANATION);
            buildExplanation(builder, getExplanation());
        }
        if (innerHits != null) {
            builder.startObject(Fields.INNER_HITS);
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    // All fields on the root level of the parsed SearhHit are interpreted as metadata fields
    // public because we use it in a completion suggestion option
    public static final ObjectParser.UnknownFieldConsumer<Map<String, Object>> unknownMetaFieldConsumer = (map, fieldName, fieldValue) -> {
        Map<String, DocumentField> fieldMap = (Map<String, DocumentField>) map.computeIfAbsent(
            METADATA_FIELDS,
            v -> new HashMap<String, DocumentField>()
        );
        if (fieldName.equals(IgnoredFieldMapper.NAME)) {
            fieldMap.put(fieldName, new DocumentField(fieldName, (List<Object>) fieldValue));
        } else {
            fieldMap.put(fieldName, new DocumentField(fieldName, Collections.singletonList(fieldValue)));
        }
    };

    /**
     * This parser outputs a temporary map of the objects needed to create the
     * SearchHit instead of directly creating the SearchHit. The reason for this
     * is that this way we can reuse the parser when parsing xContent from
     * {@link org.opensearch.search.suggest.completion.CompletionSuggestion.Entry.Option} which unfortunately inlines
     * the output of
     * {@link #toInnerXContent(XContentBuilder, ToXContent.Params)}
     * of the included search hit. The output of the map is used to create the
     * actual SearchHit instance via {@link #createFromMap(Map)}
     */
    private static final ObjectParser<Map<String, Object>, Void> MAP_PARSER = new ObjectParser<>(
        "innerHitParser",
        unknownMetaFieldConsumer,
        HashMap::new
    );

    static {
        declareInnerHitsParseFields(MAP_PARSER);
    }

    public static SearchHit fromXContent(XContentParser parser) {
        return createFromMap(MAP_PARSER.apply(parser, null));
    }

    public static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
        parser.declareString((map, value) -> map.put(Fields._INDEX, value), new ParseField(Fields._INDEX));
        parser.declareString((map, value) -> map.put(Fields._ID, value), new ParseField(Fields._ID));
        parser.declareString((map, value) -> map.put(Fields._NODE, value), new ParseField(Fields._NODE));
        parser.declareField(
            (map, value) -> map.put(Fields._SCORE, value),
            SearchHit::parseScore,
            new ParseField(Fields._SCORE),
            ValueType.FLOAT_OR_NULL
        );
        parser.declareLong((map, value) -> map.put(Fields._VERSION, value), new ParseField(Fields._VERSION));
        parser.declareLong((map, value) -> map.put(Fields._SEQ_NO, value), new ParseField(Fields._SEQ_NO));
        parser.declareLong((map, value) -> map.put(Fields._PRIMARY_TERM, value), new ParseField(Fields._PRIMARY_TERM));
        parser.declareField(
            (map, value) -> map.put(Fields._SHARD, value),
            (p, c) -> ShardId.fromString(p.text()),
            new ParseField(Fields._SHARD),
            ValueType.STRING
        );
        parser.declareObject(
            (map, value) -> map.put(SourceFieldMapper.NAME, value),
            (p, c) -> parseSourceBytes(p),
            new ParseField(SourceFieldMapper.NAME)
        );
        parser.declareObject(
            (map, value) -> map.put(Fields.HIGHLIGHT, value),
            (p, c) -> parseHighlightFields(p),
            new ParseField(Fields.HIGHLIGHT)
        );
        parser.declareObject((map, value) -> {
            Map<String, DocumentField> fieldMap = get(Fields.FIELDS, map, new HashMap<String, DocumentField>());
            fieldMap.putAll(value);
            map.put(DOCUMENT_FIELDS, fieldMap);
        }, (p, c) -> parseFields(p), new ParseField(Fields.FIELDS));
        parser.declareObject(
            (map, value) -> map.put(Fields._EXPLANATION, value),
            (p, c) -> parseExplanation(p),
            new ParseField(Fields._EXPLANATION)
        );
        parser.declareObject(
            (map, value) -> map.put(NestedIdentity._NESTED, value),
            NestedIdentity::fromXContent,
            new ParseField(NestedIdentity._NESTED)
        );
        parser.declareObject(
            (map, value) -> map.put(Fields.INNER_HITS, value),
            (p, c) -> parseInnerHits(p),
            new ParseField(Fields.INNER_HITS)
        );
        parser.declareField((p, map, context) -> {
            XContentParser.Token token = p.currentToken();
            Map<String, Float> matchedQueries = new LinkedHashMap<>();
            if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = null;
                while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = p.currentName();
                    } else if (token.isValue()) {
                        matchedQueries.put(fieldName, p.floatValue());
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                    matchedQueries.put(p.text(), Float.NaN);
                }
            } else {
                throw new IllegalStateException("expected object or array but got [" + token + "]");
            }
            map.put(Fields.MATCHED_QUERIES, matchedQueries);
        }, new ParseField(Fields.MATCHED_QUERIES), ObjectParser.ValueType.OBJECT_ARRAY);
        parser.declareField(
            (map, list) -> map.put(Fields.SORT, list),
            SearchSortValues::fromXContent,
            new ParseField(Fields.SORT),
            ValueType.OBJECT_ARRAY
        );
    }

    public static SearchHit createFromMap(Map<String, Object> values) {
        String id = get(Fields._ID, values, null);
        NestedIdentity nestedIdentity = get(NestedIdentity._NESTED, values, null);
        Map<String, DocumentField> metaFields = get(METADATA_FIELDS, values, Collections.emptyMap());
        Map<String, DocumentField> documentFields = get(DOCUMENT_FIELDS, values, Collections.emptyMap());

        SearchHit searchHit = new SearchHit(-1, id, nestedIdentity, documentFields, metaFields);
        String index = get(Fields._INDEX, values, null);
        String clusterAlias = null;
        if (index != null) {
            int indexOf = index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
            if (indexOf > 0) {
                clusterAlias = index.substring(0, indexOf);
                index = index.substring(indexOf + 1);
            }
        }
        ShardId shardId = get(Fields._SHARD, values, null);
        String nodeId = get(Fields._NODE, values, null);
        if (shardId != null && nodeId != null) {
            assert shardId.getIndexName().equals(index);
            searchHit.shard(new SearchShardTarget(nodeId, shardId, clusterAlias, OriginalIndices.NONE));
        } else {
            // these fields get set anyway when setting the shard target,
            // but we set them explicitly when we don't have enough info to rebuild the shard target
            searchHit.index = index;
            searchHit.clusterAlias = clusterAlias;
        }
        searchHit.score(get(Fields._SCORE, values, DEFAULT_SCORE));
        searchHit.version(get(Fields._VERSION, values, -1L));
        searchHit.setSeqNo(get(Fields._SEQ_NO, values, SequenceNumbers.UNASSIGNED_SEQ_NO));
        searchHit.setPrimaryTerm(get(Fields._PRIMARY_TERM, values, SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
        searchHit.sortValues(get(Fields.SORT, values, SearchSortValues.EMPTY));
        searchHit.highlightFields(get(Fields.HIGHLIGHT, values, null));
        searchHit.sourceRef(get(SourceFieldMapper.NAME, values, null));
        searchHit.explanation(get(Fields._EXPLANATION, values, null));
        searchHit.setInnerHits(get(Fields.INNER_HITS, values, null));
        searchHit.matchedQueriesWithScores(get(Fields.MATCHED_QUERIES, values, null));
        return searchHit;
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
        return (T) map.getOrDefault(key, defaultValue);
    }

    private static float parseScore(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER || parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.floatValue();
        } else {
            return Float.NaN;
        }
    }

    private static BytesReference parseSourceBytes(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
            // the original document gets slightly modified: whitespaces or
            // pretty printing are not preserved,
            // it all depends on the current builder settings
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static Map<String, DocumentField> parseFields(XContentParser parser) throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            DocumentField field = DocumentField.fromXContent(parser);
            fields.put(field.getName(), field);
        }
        return fields;
    }

    private static Map<String, SearchHits> parseInnerHits(XContentParser parser) throws IOException {
        Map<String, SearchHits> innerHits = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String name = parser.currentName();
            ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchHits.Fields.HITS);
            innerHits.put(name, SearchHits.fromXContent(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
        return innerHits;
    }

    private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            HighlightField highlightField = HighlightField.fromXContent(parser);
            highlightFields.put(highlightField.getName(), highlightField);
        }
        return highlightFields;
    }

    private static Explanation parseExplanation(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParser.Token token;
        Float value = null;
        String description = null;
        List<Explanation> details = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (Fields.VALUE.equals(currentFieldName)) {
                value = parser.floatValue();
            } else if (Fields.DESCRIPTION.equals(currentFieldName)) {
                description = parser.textOrNull();
            } else if (Fields.DETAILS.equals(currentFieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    details.add(parseExplanation(parser));
                }
            } else {
                parser.skipChildren();
            }
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation value");
        }
        if (description == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation description");
        }
        return Explanation.match(value, description, details);
    }

    private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.startObject();
        builder.field(Fields.VALUE, explanation.getValue());
        builder.field(Fields.DESCRIPTION, explanation.getDescription());
        Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            builder.startArray(Fields.DETAILS);
            for (Explanation exp : innerExps) {
                buildExplanation(builder, exp);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchHit other = (SearchHit) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(nestedIdentity, other.nestedIdentity)
            && Objects.equals(version, other.version)
            && Objects.equals(seqNo, other.seqNo)
            && Objects.equals(primaryTerm, other.primaryTerm)
            && Objects.equals(source, other.source)
            && Objects.equals(documentFields, other.documentFields)
            && Objects.equals(metaFields, other.metaFields)
            && Objects.equals(getHighlightFields(), other.getHighlightFields())
            && Objects.equals(getMatchedQueriesAndScores(), other.getMatchedQueriesAndScores())
            && Objects.equals(explanation, other.explanation)
            && Objects.equals(shard, other.shard)
            && Objects.equals(innerHits, other.innerHits)
            && Objects.equals(index, other.index)
            && Objects.equals(clusterAlias, other.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            nestedIdentity,
            version,
            seqNo,
            primaryTerm,
            source,
            documentFields,
            metaFields,
            getHighlightFields(),
            getMatchedQueriesAndScores(),
            explanation,
            shard,
            innerHits,
            index,
            clusterAlias
        );
    }

    /**
     * Encapsulates the nested identity of a hit.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static final class NestedIdentity implements Writeable, ToXContentFragment {

        private static final String _NESTED = "_nested";
        private static final String FIELD = "field";
        private static final String OFFSET = "offset";

        private final Text field;
        private final int offset;
        private final NestedIdentity child;

        public NestedIdentity(String field, int offset, NestedIdentity child) {
            this.field = new Text(field);
            this.offset = offset;
            this.child = child;
        }

        NestedIdentity(StreamInput in) throws IOException {
            field = in.readOptionalText();
            offset = in.readInt();
            child = in.readOptionalWriteable(NestedIdentity::new);
        }

        /**
         * Returns the nested field in the source this hit originates from
         */
        public Text getField() {
            return field;
        }

        /**
         * Returns the offset in the nested array of objects in the source this hit
         */
        public int getOffset() {
            return offset;
        }

        /**
         * Returns the next child nested level if there is any, otherwise <code>null</code> is returned.
         * <p>
         * In the case of mappings with multiple levels of nested object fields
         */
        public NestedIdentity getChild() {
            return child;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalText(field);
            out.writeInt(offset);
            out.writeOptionalWriteable(child);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(_NESTED);
            return innerToXContent(builder, params);
        }

        /**
         * Rendering of the inner XContent object without the leading field name. This way the structure innerToXContent renders and
         * fromXContent parses correspond to each other.
         */
        XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (field != null) {
                builder.field(FIELD, field);
            }
            if (offset != -1) {
                builder.field(OFFSET, offset);
            }
            if (child != null) {
                builder = child.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<NestedIdentity, Void> PARSER = new ConstructingObjectParser<>(
            "nested_identity",
            true,
            ctorArgs -> new NestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (NestedIdentity) ctorArgs[2])
        );
        static {
            PARSER.declareString(constructorArg(), new ParseField(FIELD));
            PARSER.declareInt(constructorArg(), new ParseField(OFFSET));
            PARSER.declareObject(optionalConstructorArg(), PARSER, new ParseField(_NESTED));
        }

        static NestedIdentity fromXContent(XContentParser parser, Void context) {
            return fromXContent(parser);
        }

        public static NestedIdentity fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            NestedIdentity other = (NestedIdentity) obj;
            return Objects.equals(field, other.field) && Objects.equals(offset, other.offset) && Objects.equals(child, other.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, offset, child);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
