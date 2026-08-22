/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Translates a {@link MultiTermsAggregationBuilder} — multi-field GROUP BY. */
public class MultiTermsBucketTranslator implements BucketTranslator<MultiTermsAggregationBuilder> {

    /** Creates a multi_terms bucket translator. */
    public MultiTermsBucketTranslator() {}

    @Override
    public Class<MultiTermsAggregationBuilder> getAggregationType() {
        return MultiTermsAggregationBuilder.class;
    }

    /** Groups by every term source in declaration order, rejecting unsupported per-source options. */
    @Override
    public GroupingInfo getGrouping(MultiTermsAggregationBuilder agg) throws ConversionException {
        List<MultiTermsValuesSourceConfig> sources = agg.terms();
        List<String> fieldNames = new ArrayList<>(sources.size());
        for (MultiTermsValuesSourceConfig source : sources) {
            rejectUnsupported(source);
            fieldNames.add(source.getFieldName());
        }
        return new FieldGrouping(fieldNames);
    }

    private static void rejectUnsupported(MultiTermsValuesSourceConfig source) throws ConversionException {
        String field = source.getFieldName();
        if (source.getMissing() != null) {
            throw new ConversionException("multi_terms does not support the 'missing' parameter on term source [" + field + "]");
        }
        if (source.getScript() != null) {
            throw new ConversionException(
                "multi_terms does not support scripted term sources; term source [" + field + "] declares a script"
            );
        }
        if (source.getIncludeExclude() != null) {
            throw new ConversionException("multi_terms does not support the 'exclude' parameter on term source [" + field + "]");
        }
        // Format and time_zone change how a key is rendered; ignoring them returns a plausible but wrong response.
        if (source.getFormat() != null) {
            throw new ConversionException("multi_terms does not support the 'format' parameter on term source [" + field + "]");
        }
        if (source.getTimeZone() != null) {
            throw new ConversionException("multi_terms does not support the 'time_zone' parameter on term source [" + field + "]");
        }
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(MultiTermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(MultiTermsAggregationBuilder agg) {
        return agg.order();
    }

    /**
     * Builds the multi_terms response, filtering nulls and min_doc_count, then sorting and truncating.
     * A {@code date} term source renders as a formatted timestamp rather than honouring the mapping's
     * format, because this path has no ValuesSource to resolve that format from.
     */
    @Override
    public InternalAggregation toBucketAggregation(MultiTermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        int arity = agg.terms().size();

        // Filter before sampling formats: a discarded entry must not decide a position's format.
        List<BucketEntry> kept = new ArrayList<>();
        for (BucketEntry entry : buckets) {
            if (hasNullKey(entry)) {
                continue;
            }
            if (entry.docCount() < agg.minDocCount()) {
                continue;
            }
            if (entry.keys().size() != arity) {
                throw new IllegalStateException(
                    "multi_terms ["
                        + agg.getName()
                        + "] expected "
                        + arity
                        + " key(s) per bucket but the result row supplied "
                        + entry.keys().size()
                );
            }
            kept.add(entry);
        }

        List<DocValueFormat> formats = sampleFormats(kept, arity);

        List<InternalMultiTerms.Bucket> termBuckets = new ArrayList<>(kept.size());
        for (BucketEntry entry : kept) {
            List<Object> values = new ArrayList<>(arity);
            for (int i = 0; i < arity; i++) {
                values.add(TermsBucketSupport.typedTerm(entry.keys().get(i)).value());
            }
            termBuckets.add(new InternalMultiTerms.Bucket(values, entry.docCount(), entry.subAggs(), false, 0, formats));
        }

        TermsBucketSupport.Truncated<InternalMultiTerms.Bucket> visible = TermsBucketSupport.sortAndTruncate(
            termBuckets,
            agg.order(),
            agg.size()
        );

        BucketOrder order = agg.order();
        return new InternalMultiTerms(
            agg.getName(),
            order, // reduceOrder
            order, // display order
            AggregationTranslator.userMetadata(agg),
            agg.shardSize(),
            false, // show_term_doc_count_error
            visible.otherDocCount(),
            0,
            formats,
            visible.buckets(),
            thresholds(agg)
        );
    }

    private static boolean hasNullKey(BucketEntry entry) {
        for (Object key : entry.keys()) {
            if (key == null) {
                return true;
            }
        }
        return false;
    }

    /** Samples one format per position from the first surviving entry, falling back to RAW. */
    private static List<DocValueFormat> sampleFormats(List<BucketEntry> kept, int arity) {
        BucketEntry sample = kept.isEmpty() ? null : kept.get(0);
        List<DocValueFormat> formats = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            formats.add(sample == null ? DocValueFormat.RAW : TermsBucketSupport.typedTerm(sample.keys().get(i)).format());
        }
        return formats;
    }

    /** Bundles the request's bucket-count knobs for the result constructor. */
    private static TermsAggregator.BucketCountThresholds thresholds(MultiTermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize());
    }
}
