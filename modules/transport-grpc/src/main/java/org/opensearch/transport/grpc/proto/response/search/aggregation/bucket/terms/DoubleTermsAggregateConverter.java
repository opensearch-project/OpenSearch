package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;

import java.io.IOException;

import static org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils.newValue;

/**
 * Proto converter for {@link DoubleTerms}
 */
public class DoubleTermsAggregateConverter extends TermsAggregatesProtoConverter<DoubleTerms.Bucket> {
    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return DoubleTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        DoubleTerms doubleTerms = (DoubleTerms) aggregation;
        Aggregate.Builder protoBuilder = Aggregate.newBuilder();
        convertCommon(protoBuilder, doubleTerms.getDocCountError(), doubleTerms.getSumOfOtherDocCounts(), doubleTerms.getBuckets());
        return protoBuilder;
    }

    /**
     * Mirroring {@link DoubleTerms.Bucket#keyToXContent(XContentBuilder)}
     *
     * {@inheritDoc}
     */
    @Override
    void convertBucketKey(ObjectMap.Builder builder, DoubleTerms.Bucket bucket) {
        builder.putFields(Aggregation.CommonFields.KEY.getPreferredName(), newValue((double) bucket.getKey()));
        if (bucket.getFormat() != DocValueFormat.RAW) {
            builder.putFields(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName(), newValue(bucket.getKeyAsString()));
        }
    }
}
