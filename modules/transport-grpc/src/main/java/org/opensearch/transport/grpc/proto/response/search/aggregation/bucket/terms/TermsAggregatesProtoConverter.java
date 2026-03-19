package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;
import java.util.List;

import static org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils.newValue;

/**
 * A common base implementation for TermsAggregation, child classes will only need to implement {@link #convertBucketKey(ObjectMap.Builder, InternalTerms.Bucket)}
 * and call {@link #convertCommon(Aggregate.Builder, long, long, List)} accordingly
 *
 * @param <B> bucket type for each subclass
 */
public abstract class TermsAggregatesProtoConverter<B extends InternalTerms.Bucket<B>> implements AggregateProtoConverter {

    /**
     * Mirroring {@link InternalMappedTerms#doXContentBody(XContentBuilder, ToXContent.Params)}
     */
    public void convertCommon(Aggregate.Builder protoBuilder, long docCountError, long otherDocCount, List<B> buckets) throws IOException {
        protoBuilder.setDocCountErrorUpperBound(docCountError);
        protoBuilder.setSumOtherDocCount(otherDocCount);
        for (B bucket: buckets) {
            protoBuilder.addBuckets(convertBucket(bucket));
        }
    }

    /**
     * Mirroring {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms.Bucket#toXContent(XContentBuilder, ToXContent.Params)}
     */
    public ObjectMap convertBucket(B bucket) throws IOException {
        ObjectMap.Builder builder = ObjectMap.newBuilder();
        convertBucketKey(builder, bucket);
        builder.putFields(
                Aggregation.CommonFields.DOC_COUNT.getPreferredName(),
                newValue(bucket.getDocCount())
        );
        if (bucket.showDocCountError()) {
            builder.putFields(
                    InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(),
                    newValue(bucket.getDocCountError())
            );
        }
        for (Aggregation aggregation: bucket.getAggregations()) {
            if (aggregation instanceof InternalAggregation internalAggregation) {
                Aggregate aggregate = AggregateProtoUtils.toProto(internalAggregation);
                // NOTE: this is slightly different from InternalAggregation#toXContent
                // where the name might be joined together with the type depending on the parameter
                // of toXContent
                builder.putFields(aggregation.getName(), newValue(aggregateToObjectMap(aggregate)));
            }
        }
        return builder.build();
    }

    abstract void convertBucketKey(ObjectMap.Builder builder, B bucket);

    private static ObjectMap aggregateToObjectMap(Aggregate aggregate) {
        ObjectMap.Builder builder = ObjectMap.newBuilder();
        builder.putFields("meta", newValue(aggregate.getMeta()));
        ObjectMap.ListValue.Builder bucketsBuilder = ObjectMap.ListValue.newBuilder();
        for (ObjectMap bucket: aggregate.getBucketsList()) {
            bucketsBuilder.addValue(newValue(bucket));
        }
        builder.putFields("buckets", newValue(bucketsBuilder.build()));
        builder.putFields("doc_count_error_upper_bound", newValue(aggregate.getDocCountErrorUpperBound()));
        builder.putFields("sum_other_doc_count", newValue(aggregate.getSumOtherDocCount()));
        if (aggregate.getValue().hasDouble()) {
            builder.putFields("value", newValue(aggregate.getValue().getDouble()));
        } else {
            builder.putFields("value", newValue(aggregate.getValue().getNullValue()));
        }
        builder.putFields("value_as_string", newValue(aggregate.getValueAsString()));
        return builder.build();
    }
}
