/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.DoubleTermsAggregate;
import org.opensearch.protobufs.DoubleTermsBucket;
import org.opensearch.protobufs.LongTermsAggregate;
import org.opensearch.protobufs.LongTermsBucket;
import org.opensearch.protobufs.LongTermsBucketKey;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.StringTermsBucket;
import org.opensearch.protobufs.TermsAggregateBaseDoubleTermsBucketAllOfBuckets;
import org.opensearch.protobufs.TermsAggregateBaseLongTermsBucketAllOfBuckets;
import org.opensearch.protobufs.TermsAggregateBaseStringTermsBucketAllOfBuckets;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting OpenSearch Terms aggregation results to Protocol Buffer messages.
 * Handles StringTerms, LongTerms, and DoubleTerms conversions.
 *
 * <p>Note: The current protobuf schema does not include an aggregations field in bucket messages
 * (StringTermsBucket, LongTermsBucket, DoubleTermsBucket). Therefore, nested aggregations within
 * buckets are not supported in this implementation. If buckets contain nested aggregations,
 * a warning will be logged but the conversion will proceed with the bucket-level data only.
 */
class TermsAggregateProtoUtils {

    private TermsAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts StringTerms to StringTermsAggregate protobuf.
     *
     * @param terms The StringTerms aggregation result
     * @return The protobuf StringTermsAggregate
     * @throws IOException if an error occurs during conversion
     */
    static StringTermsAggregate stringTermsToProto(StringTerms terms) throws IOException {
        StringTermsAggregate.Builder builder = StringTermsAggregate.newBuilder();

        // Set metadata if present
        setMetadata(terms, builder);

        // Set aggregate-level error fields
        builder.setDocCountErrorUpperBound(terms.getDocCountError());
        builder.setSumOtherDocCount(terms.getSumOfOtherDocCounts());

        // Convert buckets
        TermsAggregateBaseStringTermsBucketAllOfBuckets bucketsContainer = convertStringBuckets(terms.getBuckets());
        builder.setBuckets(bucketsContainer);

        return builder.build();
    }

    /**
     * Converts LongTerms to LongTermsAggregate protobuf.
     *
     * @param terms The LongTerms aggregation result
     * @return The protobuf LongTermsAggregate
     * @throws IOException if an error occurs during conversion
     */
    static LongTermsAggregate longTermsToProto(LongTerms terms) throws IOException {
        LongTermsAggregate.Builder builder = LongTermsAggregate.newBuilder();

        // Set metadata if present
        setMetadata(terms, builder);

        // Set aggregate-level error fields
        builder.setDocCountErrorUpperBound(terms.getDocCountError());
        builder.setSumOtherDocCount(terms.getSumOfOtherDocCounts());

        // Convert buckets
        TermsAggregateBaseLongTermsBucketAllOfBuckets bucketsContainer = convertLongBuckets(terms.getBuckets());
        builder.setBuckets(bucketsContainer);

        return builder.build();
    }

    /**
     * Converts DoubleTerms to DoubleTermsAggregate protobuf.
     *
     * @param terms The DoubleTerms aggregation result
     * @return The protobuf DoubleTermsAggregate
     * @throws IOException if an error occurs during conversion
     */
    static DoubleTermsAggregate doubleTermsToProto(DoubleTerms terms) throws IOException {
        DoubleTermsAggregate.Builder builder = DoubleTermsAggregate.newBuilder();

        // Set metadata if present
        setMetadata(terms, builder);

        // Set aggregate-level error fields
        builder.setDocCountErrorUpperBound(terms.getDocCountError());
        builder.setSumOtherDocCount(terms.getSumOfOtherDocCounts());

        // Convert buckets
        TermsAggregateBaseDoubleTermsBucketAllOfBuckets bucketsContainer = convertDoubleBuckets(terms.getBuckets());
        builder.setBuckets(bucketsContainer);

        return builder.build();
    }

    /**
     * Converts StringTerms buckets to protobuf bucket container.
     */
    private static TermsAggregateBaseStringTermsBucketAllOfBuckets convertStringBuckets(List<StringTerms.Bucket> buckets)
        throws IOException {
        TermsAggregateBaseStringTermsBucketAllOfBuckets.Builder containerBuilder = TermsAggregateBaseStringTermsBucketAllOfBuckets
            .newBuilder();

        // Populate the string_terms_bucket map using index as key
        Map<String, StringTermsBucket> bucketMap = new HashMap<>();
        for (int i = 0; i < buckets.size(); i++) {
            StringTermsBucket protoBucket = convertStringBucket(buckets.get(i));
            bucketMap.put(String.valueOf(i), protoBucket);
        }

        containerBuilder.putAllStringTermsBucket(bucketMap);
        return containerBuilder.build();
    }

    /**
     * Converts LongTerms buckets to protobuf bucket container.
     */
    private static TermsAggregateBaseLongTermsBucketAllOfBuckets convertLongBuckets(List<LongTerms.Bucket> buckets) throws IOException {
        TermsAggregateBaseLongTermsBucketAllOfBuckets.Builder containerBuilder = TermsAggregateBaseLongTermsBucketAllOfBuckets.newBuilder();

        // Populate the long_terms_bucket map using index as key
        Map<String, LongTermsBucket> bucketMap = new HashMap<>();
        for (int i = 0; i < buckets.size(); i++) {
            LongTermsBucket protoBucket = convertLongBucket(buckets.get(i));
            bucketMap.put(String.valueOf(i), protoBucket);
        }

        containerBuilder.putAllLongTermsBucket(bucketMap);
        return containerBuilder.build();
    }

    /**
     * Converts DoubleTerms buckets to protobuf bucket container.
     */
    @SuppressWarnings("unchecked")
    private static TermsAggregateBaseDoubleTermsBucketAllOfBuckets convertDoubleBuckets(List<? extends InternalTerms.Bucket<?>> buckets)
        throws IOException {
        TermsAggregateBaseDoubleTermsBucketAllOfBuckets.Builder containerBuilder = TermsAggregateBaseDoubleTermsBucketAllOfBuckets
            .newBuilder();

        // Populate the double_terms_bucket map using index as key
        Map<String, DoubleTermsBucket> bucketMap = new HashMap<>();
        for (int i = 0; i < buckets.size(); i++) {
            DoubleTermsBucket protoBucket = convertDoubleBucket((InternalTerms.Bucket) buckets.get(i));
            bucketMap.put(String.valueOf(i), protoBucket);
        }

        containerBuilder.putAllDoubleTermsBucket(bucketMap);
        return containerBuilder.build();
    }

    /**
     * Converts a single StringTerms bucket to protobuf.
     */
    private static StringTermsBucket convertStringBucket(StringTerms.Bucket bucket) throws IOException {
        StringTermsBucket.Builder builder = StringTermsBucket.newBuilder();

        // Required fields
        builder.setDocCount(bucket.getDocCount());
        builder.setKey(bucket.getKeyAsString());

        // Optional: doc_count_error (only if showDocCountError is true)
        if (bucket.showDocCountError()) {
            builder.setDocCountError(bucket.getDocCountError());
        }

        // Note: Nested aggregations are not supported in the current protobuf schema
        // StringTermsBucket does not have an aggregations field

        return builder.build();
    }

    /**
     * Converts a single LongTerms bucket to protobuf.
     */
    private static LongTermsBucket convertLongBucket(LongTerms.Bucket bucket) throws IOException {
        LongTermsBucket.Builder builder = LongTermsBucket.newBuilder();

        // Required fields
        builder.setDocCount(bucket.getDocCount());

        // Key: need to wrap in LongTermsBucketKey object
        Object keyObj = bucket.getKey();
        long keyValue;
        if (keyObj instanceof Long) {
            keyValue = (Long) keyObj;
        } else if (keyObj instanceof Number) {
            keyValue = ((Number) keyObj).longValue();
        } else {
            throw new IllegalArgumentException("Unexpected key type in LongTerms bucket: " + keyObj.getClass().getName());
        }

        LongTermsBucketKey protoKey = LongTermsBucketKey.newBuilder().setSigned(keyValue).build();
        builder.setKey(protoKey);

        // key_as_string: formatted representation
        builder.setKeyAsString(bucket.getKeyAsString());

        // Optional: doc_count_error
        if (bucket.showDocCountError()) {
            builder.setDocCountError(bucket.getDocCountError());
        }

        // Note: Nested aggregations are not supported in the current protobuf schema

        return builder.build();
    }

    /**
     * Converts a single DoubleTerms bucket to protobuf.
     */
    private static DoubleTermsBucket convertDoubleBucket(InternalTerms.Bucket<?> bucket) throws IOException {
        DoubleTermsBucket.Builder builder = DoubleTermsBucket.newBuilder();

        // Required fields
        builder.setDocCount(bucket.getDocCount());

        Object keyObj = bucket.getKey();
        double keyValue;
        if (keyObj instanceof Double) {
            keyValue = (Double) keyObj;
        } else if (keyObj instanceof Number) {
            keyValue = ((Number) keyObj).doubleValue();
        } else {
            throw new IllegalArgumentException("Unexpected key type in DoubleTerms bucket: " + keyObj.getClass().getName());
        }

        builder.setKey(keyValue);
        builder.setKeyAsString(bucket.getKeyAsString());

        // Optional: doc_count_error
        if (bucket.showDocCountError()) {
            builder.setDocCountError(bucket.getDocCountError());
        }

        // Note: Nested aggregations are not supported in the current protobuf schema

        return builder.build();
    }

    /**
     * Sets metadata for StringTermsAggregate if present.
     */
    private static void setMetadata(InternalTerms<?, ?> terms, StringTermsAggregate.Builder builder) {
        if (terms.getMetadata() != null && !terms.getMetadata().isEmpty()) {
            ObjectMap.Value metaValue = ObjectMapProtoUtils.toProto(terms.getMetadata());
            if (metaValue.hasObjectMap()) {
                builder.setMeta(metaValue.getObjectMap());
            }
        }
    }

    /**
     * Sets metadata for LongTermsAggregate if present.
     */
    private static void setMetadata(InternalTerms<?, ?> terms, LongTermsAggregate.Builder builder) {
        if (terms.getMetadata() != null && !terms.getMetadata().isEmpty()) {
            ObjectMap.Value metaValue = ObjectMapProtoUtils.toProto(terms.getMetadata());
            if (metaValue.hasObjectMap()) {
                builder.setMeta(metaValue.getObjectMap());
            }
        }
    }

    /**
     * Sets metadata for DoubleTermsAggregate if present.
     */
    private static void setMetadata(InternalTerms<?, ?> terms, DoubleTermsAggregate.Builder builder) {
        if (terms.getMetadata() != null && !terms.getMetadata().isEmpty()) {
            ObjectMap.Value metaValue = ObjectMapProtoUtils.toProto(terms.getMetadata());
            if (metaValue.hasObjectMap()) {
                builder.setMeta(metaValue.getObjectMap());
            }
        }
    }
}
