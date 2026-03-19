package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;

import java.io.IOException;

import static org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils.newValue;

/**
 * Proto converter for {@link LongTerms}
 */
public class LongTermsAggregateConverter extends TermsAggregatesProtoConverter<LongTerms.Bucket> {
    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return LongTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        LongTerms longTerms = (LongTerms) aggregation;
        Aggregate.Builder protoBuilder = Aggregate.newBuilder();
        convertCommon(protoBuilder, longTerms.getDocCountError(), longTerms.getSumOfOtherDocCounts(), longTerms.getBuckets());
        return protoBuilder;
    }

    /**
     * Mirroring {@link LongTerms.Bucket#keyToXContent(XContentBuilder)}
     *
     * {@inheritDoc}
     */
    @Override
    void convertBucketKey(ObjectMap.Builder builder, LongTerms.Bucket bucket) {
        Object key = bucket.getKey();
        // the key could be a long or a BigInteger produced by UNSIGNED_LONG_SHIFTED
        if (key instanceof Long) {
            builder.putFields(Aggregation.CommonFields.KEY.getPreferredName(), newValue((long) key));
        } else {
            // BigInteger's case, use string to represent
            builder.putFields(Aggregation.CommonFields.KEY.getPreferredName(), newValue(key.toString()));
        }
        if (bucket.getFormat() != DocValueFormat.RAW && bucket.getFormat() != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
            builder.putFields(Aggregation.CommonFields.KEY_AS_STRING.getPreferredName(), newValue(bucket.getKeyAsString()));
        }
    }
}
