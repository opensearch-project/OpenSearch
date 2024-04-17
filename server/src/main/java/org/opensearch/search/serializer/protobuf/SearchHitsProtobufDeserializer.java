/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer.protobuf;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.serializer.SearchHitsDeserializer;
import org.opensearch.server.proto.FetchSearchResultProto;
import org.opensearch.server.proto.QuerySearchResultProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Deserializer for {@link SearchHits} to/from protobuf.
 */
public class SearchHitsProtobufDeserializer implements SearchHitsDeserializer<InputStream> {

    private FetchSearchResultProto.SearchHits searchHitsProto;

    @Override
    public SearchHits createSearchHits(InputStream inputStream) throws IOException {
        this.searchHitsProto = FetchSearchResultProto.SearchHits.parseFrom(inputStream);
        SearchHit[] hits = new SearchHit[this.searchHitsProto.getHitsCount()];
        SearchHitProtobufDeserializer protobufSerializer = new SearchHitProtobufDeserializer();
        for (int i = 0; i < this.searchHitsProto.getHitsCount(); i++) {
            hits[i] = protobufSerializer.createSearchHit(new ByteArrayInputStream(this.searchHitsProto.getHits(i).toByteArray()));
        }
        TotalHits totalHits = new TotalHits(
            this.searchHitsProto.getTotalHits().getValue(),
            Relation.valueOf(this.searchHitsProto.getTotalHits().getRelation().toString())
        );
        float maxScore = this.searchHitsProto.getMaxScore();
        SortField[] sortFields = this.searchHitsProto.getSortFieldsList()
            .stream()
            .map(sortField -> new SortField(sortField.getField(), SortField.Type.valueOf(sortField.getType().toString())))
            .toArray(SortField[]::new);
        String collapseField = this.searchHitsProto.getCollapseField();
        Object[] collapseValues = new Object[this.searchHitsProto.getCollapseValuesCount()];
        for (int i = 0; i < this.searchHitsProto.getCollapseValuesCount(); i++) {
            collapseValues[i] = readSortValueFromProtobuf(this.searchHitsProto.getCollapseValues(i));
        }
        return new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues);
    }

    public static Object readSortValueFromProtobuf(FetchSearchResultProto.SortValue collapseValue) throws IOException {
        if (collapseValue.hasCollapseString()) {
            return collapseValue.getCollapseString();
        } else if (collapseValue.hasCollapseInt()) {
            return collapseValue.getCollapseInt();
        } else if (collapseValue.hasCollapseLong()) {
            return collapseValue.getCollapseLong();
        } else if (collapseValue.hasCollapseFloat()) {
            return collapseValue.getCollapseFloat();
        } else if (collapseValue.hasCollapseDouble()) {
            return collapseValue.getCollapseDouble();
        } else if (collapseValue.hasCollapseBytes()) {
            return new BytesRef(collapseValue.getCollapseBytes().toByteArray());
        } else if (collapseValue.hasCollapseBool()) {
            return collapseValue.getCollapseBool();
        } else {
            throw new IOException("Can't handle sort field value of type [" + collapseValue + "]");
        }
    }

    public static FetchSearchResultProto.SearchHits convertHitsToProto(SearchHits hits) {
        List<FetchSearchResultProto.SearchHit> searchHitList = new ArrayList<>();
        for (SearchHit hit : hits) {
            searchHitList.add(SearchHitProtobufDeserializer.convertHitToProto(hit));
        }
        QuerySearchResultProto.TotalHits.Builder totalHitsBuilder = QuerySearchResultProto.TotalHits.newBuilder();
        if (hits.getTotalHits() != null) {
            totalHitsBuilder.setValue(hits.getTotalHits().value);
            totalHitsBuilder.setRelation(
                hits.getTotalHits().relation == Relation.EQUAL_TO
                    ? QuerySearchResultProto.TotalHits.Relation.EQUAL_TO
                    : QuerySearchResultProto.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            );
        }
        FetchSearchResultProto.SearchHits.Builder searchHitsBuilder = FetchSearchResultProto.SearchHits.newBuilder();
        searchHitsBuilder.setMaxScore(hits.getMaxScore());
        searchHitsBuilder.addAllHits(searchHitList);
        searchHitsBuilder.setTotalHits(totalHitsBuilder.build());
        if (hits.getSortFields() != null && hits.getSortFields().length > 0) {
            for (SortField sortField : hits.getSortFields()) {
                FetchSearchResultProto.SortField.Builder sortFieldBuilder = FetchSearchResultProto.SortField.newBuilder();
                if (sortField.getField() != null) {
                    sortFieldBuilder.setField(sortField.getField());
                }
                sortFieldBuilder.setType(FetchSearchResultProto.SortField.Type.valueOf(sortField.getType().name()));
                searchHitsBuilder.addSortFields(sortFieldBuilder.build());
            }
        }
        if (hits.getCollapseField() != null) {
            searchHitsBuilder.setCollapseField(hits.getCollapseField());
            for (Object value : hits.getCollapseValues()) {
                FetchSearchResultProto.SortValue.Builder collapseValueBuilder = FetchSearchResultProto.SortValue.newBuilder();
                try {
                    collapseValueBuilder = readSortValueForProtobuf(value, collapseValueBuilder);
                } catch (IOException e) {
                    throw new OpenSearchException(e);
                }
                searchHitsBuilder.addCollapseValues(collapseValueBuilder.build());
            }
        }
        return searchHitsBuilder.build();
    }

    public static FetchSearchResultProto.SortValue.Builder readSortValueForProtobuf(
        Object collapseValue,
        FetchSearchResultProto.SortValue.Builder collapseValueBuilder
    ) throws IOException {
        Class type = collapseValue.getClass();
        if (type == String.class) {
            collapseValueBuilder.setCollapseString((String) collapseValue);
        } else if (type == Integer.class || type == Short.class) {
            collapseValueBuilder.setCollapseInt((Integer) collapseValue);
        } else if (type == Long.class) {
            collapseValueBuilder.setCollapseLong((Long) collapseValue);
        } else if (type == Float.class) {
            collapseValueBuilder.setCollapseFloat((Float) collapseValue);
        } else if (type == Double.class) {
            collapseValueBuilder.setCollapseDouble((Double) collapseValue);
        } else if (type == Byte.class) {
            byte b = (Byte) collapseValue;
            collapseValueBuilder.setCollapseBytes(ByteString.copyFrom(new byte[] { b }));
        } else if (type == Boolean.class) {
            collapseValueBuilder.setCollapseBool((Boolean) collapseValue);
        } else if (type == BytesRef.class) {
            collapseValueBuilder.setCollapseBytes(ByteString.copyFrom(((BytesRef) collapseValue).bytes));
        } else if (type == BigInteger.class) {
            BigInteger bigInt = (BigInteger) collapseValue;
            collapseValueBuilder.setCollapseBytes(ByteString.copyFrom(bigInt.toByteArray()));
        } else {
            throw new IOException("Can't handle sort field value of type [" + type + "]");
        }
        return collapseValueBuilder;
    }

}
