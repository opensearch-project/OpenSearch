/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.serde;

import org.apache.lucene.search.Explanation;
import org.opensearch.Version;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchSortValues;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.*;
import static org.opensearch.common.lucene.Lucene.readExplanation;

public class SearchHitSerDe implements SerDe.StreamSerializer<SearchHit>, SerDe.StreamDeserializer<SearchHit> {

    @Override
    public SearchHit deserialize(StreamInput in) {
        try {
            return fromStream(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(SearchHit object, StreamOutput out) throws SerDe.SerializationException {
        try {
            object.writeTo(out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }

    private SearchHit fromStream(StreamInput in) throws IOException {
        int docId;
        float score;
        long seqNo;
        long version;
        long primaryTerm;
        Text id;
        BytesReference source;
        SearchShardTarget shard;
        Explanation explanation = null;
        SearchSortValues sortValues;
        SearchHit.NestedIdentity nestedIdentity;
        Map<String, DocumentField> documentFields;
        Map<String, DocumentField> metaFields;
        Map<String, HighlightField> highlightFields;
        Map<String, Float> matchedQueries = Map.of();
        Map<String, SearchHits> innerHits;

        docId = -1;
        score = in.readFloat();
        id = in.readOptionalText();
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readOptionalText();
        }
        nestedIdentity = in.readOptionalWriteable(SearchHit.NestedIdentity::new);
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
            Map<String, HighlightField> hlFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = new HighlightField(in);
                hlFields.put(field.name(), field);
            }
            highlightFields = unmodifiableMap(hlFields);
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
        shard = in.readOptionalWriteable(SearchShardTarget::new);
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

        return new SearchHit(
            docId,
            score,
            seqNo,
            version,
            primaryTerm,
            id,
            source,
            shard,
            explanation,
            sortValues,
            nestedIdentity,
            documentFields,
            metaFields,
            highlightFields,
            matchedQueries,
            innerHits
        );
    }
}
