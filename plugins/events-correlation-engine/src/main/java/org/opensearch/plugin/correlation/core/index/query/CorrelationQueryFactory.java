/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.query;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Optional;

/**
 * CorrelationQueryFactory util class is used to construct a Lucene KnnFloatVectorQuery.
 *
 * @opensearch.internal
 */
public class CorrelationQueryFactory {

    /**
     * static method which takes input params to construct a Lucene KnnFloatVectorQuery.
     * @param createQueryRequest object parameter containing inputs for constructing Lucene KnnFloatVectorQuery.
     * @return generic Lucene Query object
     */
    public static Query create(CreateQueryRequest createQueryRequest) {
        final String indexName = createQueryRequest.getIndexName();
        final String fieldName = createQueryRequest.getFieldName();
        final int k = createQueryRequest.getK();
        final float[] vector = createQueryRequest.getVector();

        if (createQueryRequest.getFilter().isPresent()) {
            final QueryShardContext context = createQueryRequest.getContext()
                .orElseThrow(() -> new RuntimeException("Shard context cannot be null"));

            try {
                final Query filterQuery = createQueryRequest.getFilter().get().toQuery(context);
                return new KnnFloatVectorQuery(fieldName, vector, k, filterQuery);
            } catch (IOException ex) {
                throw new RuntimeException("Cannot create knn query with filter", ex);
            }
        }
        return new KnnFloatVectorQuery(fieldName, vector, k);
    }

    /**
     * class containing params to construct a Lucene KnnFloatVectorQuery.
     *
     * @opensearch.internal
     */
    public static class CreateQueryRequest {
        private String indexName;

        private String fieldName;

        private float[] vector;

        private int k;

        private QueryBuilder filter;

        private QueryShardContext context;

        /**
         * Parameterized ctor for CreateQueryRequest
         * @param indexName index name
         * @param fieldName field name
         * @param vector query vector
         * @param k number of nearby neighbors
         * @param filter additional filter query
         * @param context QueryShardContext
         */
        public CreateQueryRequest(
            String indexName,
            String fieldName,
            float[] vector,
            int k,
            QueryBuilder filter,
            QueryShardContext context
        ) {
            this.indexName = indexName;
            this.fieldName = fieldName;
            this.vector = vector;
            this.k = k;
            this.filter = filter;
            this.context = context;
        }

        /**
         * get index name
         * @return get index name
         */
        public String getIndexName() {
            return indexName;
        }

        /**
         * get field name
         * @return get field name
         */
        public String getFieldName() {
            return fieldName;
        }

        /**
         * get vector
         * @return get vector
         */
        public float[] getVector() {
            return vector;
        }

        /**
         * get number of nearby neighbors
         * @return number of nearby neighbors
         */
        public int getK() {
            return k;
        }

        /**
         * get optional filter query
         * @return get optional filter query
         */
        public Optional<QueryBuilder> getFilter() {
            return Optional.ofNullable(filter);
        }

        /**
         * get optional query shard context
         * @return get optional query shard context
         */
        public Optional<QueryShardContext> getContext() {
            return Optional.ofNullable(context);
        }
    }
}
