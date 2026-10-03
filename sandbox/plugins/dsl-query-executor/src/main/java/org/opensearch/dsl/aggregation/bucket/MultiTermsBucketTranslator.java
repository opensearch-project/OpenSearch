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
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
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
import java.util.function.Supplier;

/**
 * Translates a {@link MultiTermsAggregationBuilder} — multi-field GROUP BY.
 * {@code {"aggs": {"combo": {"multi_terms": {"terms": [{"field": "brand"}, {"field": "status"}]}}}}}
 * becomes {@code GROUP BY brand, status}.
 *
 * <p>Unlike single-field {@code terms}, a multi_terms grouping is multi-field, and the plan's
 * bucket-count machinery (fetch, per-parent bounds, eligible-doc counts) is single-field today,
 * so a multi_terms plan is <em>unbounded</em>: it groups and sorts but does not truncate. This
 * translator therefore renders through the base-contract path and applies {@code min_doc_count},
 * the requested order, and the top-{@code size} cut itself, reporting the truncated tail as
 * {@code sum_other_doc_count}.
 *
 * <p>Keys are heterogeneous — one value per term source, in declaration order — so each position
 * is converted by its runtime type (number, boolean, binary) into the raw value
 * {@link InternalMultiTerms} stores, paired with the {@link DocValueFormat} that renders it. The
 * per-position format is resolved from the source field's mapping; a field whose mapping cannot
 * be resolved at render time fails the request. Under a RAW-resolved format, binary (ip) keys are
 * pre-rendered as address strings.
 */
public class MultiTermsBucketTranslator implements BucketTranslator<MultiTermsAggregationBuilder> {

    private final Supplier<MapperService> mapperServiceSupplier;

    /**
     * Creates a multi_terms bucket translator.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService for per-source key
     *        format resolution and {@link #validate} mapping checks; supplying null skips those
     *        validate() mapping checks and fails rendering
     */
    public MultiTermsBucketTranslator(Supplier<MapperService> mapperServiceSupplier) {
        this.mapperServiceSupplier = mapperServiceSupplier;
    }

    @Override
    public Class<MultiTermsAggregationBuilder> getAggregationType() {
        return MultiTermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(MultiTermsAggregationBuilder agg) {
        List<MultiTermsValuesSourceConfig> sources = agg.terms();
        List<String> fieldNames = new ArrayList<>(sources.size());
        for (MultiTermsValuesSourceConfig source : sources) {
            fieldNames.add(source.getFieldName());
        }
        return new FieldGrouping(fieldNames);
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
     * Rejects per-source parameters the translation does not implement ({@code missing},
     * {@code script}, {@code exclude}, {@code format}, {@code time_zone}), date-mapped term
     * sources, and {@code min_doc_count: 0}: each would change the bucket set or key rendering
     * relative to classic search if ignored. Date rejection needs the mapping, so it is skipped
     * when no {@link MapperService} is supplied.
     */
    @Override
    public void validate(MultiTermsAggregationBuilder agg) throws ConversionException {
        for (MultiTermsValuesSourceConfig source : agg.terms()) {
            String field = source.getFieldName();
            if (source.getMissing() != null) {
                throw new ConversionException("multi_terms does not support the 'missing' parameter on term source [" + field + "]");
            }
            if (source.getScript() != null) {
                throw new ConversionException("multi_terms does not support the 'script' parameter on term source [" + field + "]");
            }
            if (source.getIncludeExclude() != null) {
                throw new ConversionException("multi_terms does not support the 'exclude' parameter on term source [" + field + "]");
            }
            if (source.getFormat() != null) {
                throw new ConversionException("multi_terms does not support the 'format' parameter on term source [" + field + "]");
            }
            if (source.getTimeZone() != null) {
                throw new ConversionException("multi_terms does not support the 'time_zone' parameter on term source [" + field + "]");
            }
            MappedFieldType fieldType = resolveFieldType(field);
            if (fieldType != null
                && (DateFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())
                    || DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(fieldType.typeName()))) {
                throw new ConversionException(
                    "multi_terms does not support date term source ["
                        + field
                        + "] — date bucket keys cannot yet be rendered with mapping formats"
                );
            }
        }
        if (agg.minDocCount() == 0) {
            throw new ConversionException(
                "[min_doc_count: 0] on multi_terms aggregation ["
                    + agg.getName()
                    + "] is not supported by the DSL execution path — zero-count buckets require enumerating term "
                    + "combinations that a GROUP BY over matching documents cannot produce"
            );
        }
    }

    /**
     * Renders the multi_terms response from the plan's grouped rows. The plan sorted the rows but
     * did not bound them, so this method drops entries with a null key at any position (parity
     * with classic multi_terms, since {@code missing} is rejected), applies {@code min_doc_count},
     * fails loudly on an arity mismatch, then orders, truncates to the top {@code size}, and
     * reports the truncated tail as {@code sum_other_doc_count}.
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

        List<DocValueFormat> formats = resolveFormats(agg, kept, arity);

        List<InternalMultiTerms.Bucket> termBuckets = new ArrayList<>(kept.size());
        for (BucketEntry entry : kept) {
            List<Object> values = new ArrayList<>(arity);
            for (int i = 0; i < arity; i++) {
                values.add(termValue(entry.keys().get(i), formats.get(i)));
            }
            termBuckets.add(new InternalMultiTerms.Bucket(values, entry.docCount(), entry.subAggs(), false, 0, formats));
        }

        BucketOrder order = agg.order();
        termBuckets.sort(order.comparator());
        long otherDocCount = truncate(termBuckets, agg.size());

        return new InternalMultiTerms(
            agg.getName(),
            order, // reduceOrder: the bucket list is sorted by it
            order, // the user-requested display order
            AggregationTranslator.userMetadata(agg),
            agg.shardSize(), // request echo — no shard fan-out on this path
            false, // no per-bucket doc count error rendering
            otherDocCount,
            0, // exact single-plan path: doc_count_error_upper_bound is truly 0
            formats,
            termBuckets,
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

    /**
     * Truncates {@code buckets} in place to the top {@code size}, returning the summed doc count
     * of the removed tail for {@code sum_other_doc_count}. The list must already be ordered.
     */
    private static long truncate(List<InternalMultiTerms.Bucket> buckets, int size) {
        if (buckets.size() <= size) {
            return 0L;
        }
        long otherDocCount = 0;
        for (int i = size; i < buckets.size(); i++) {
            otherDocCount += buckets.get(i).getDocCount();
        }
        buckets.subList(size, buckets.size()).clear();
        return otherDocCount;
    }

    /**
     * Resolves one {@link DocValueFormat} per term-source position. A boolean-valued position
     * always uses {@link DocValueFormat#BOOLEAN}; otherwise the position takes its source field's
     * mapping-resolved format, failing the request when that mapping cannot be resolved. A
     * position is typed from the first surviving entry, since a column is homogeneous.
     */
    private List<DocValueFormat> resolveFormats(MultiTermsAggregationBuilder agg, List<BucketEntry> kept, int arity) {
        List<MultiTermsValuesSourceConfig> sources = agg.terms();
        BucketEntry sample = kept.isEmpty() ? null : kept.get(0);
        List<DocValueFormat> formats = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            Object sampleKey = sample == null ? null : sample.keys().get(i);
            if (sampleKey instanceof Boolean) {
                formats.add(DocValueFormat.BOOLEAN);
                continue;
            }
            MappedFieldType fieldType = requireFieldType(agg, sources.get(i).getFieldName());
            formats.add(fieldType.docValueFormat(null, null));
        }
        return formats;
    }

    /**
     * Converts a single key position into the raw value {@link InternalMultiTerms.Bucket} stores,
     * matching {@link InternalMultiTerms#formatObject}: booleans become 1/0 as a {@code long},
     * floating-point numbers widen to {@code double}, other numbers narrow to {@code long}, and
     * binary/string keys are converted by {@link BinaryTermKeys#termBytes} (ip address string under
     * {@link DocValueFormat#RAW}, encoded bytes otherwise).
     */
    private static Object termValue(Object key, DocValueFormat format) {
        if (key instanceof Boolean bool) {
            return bool ? 1L : 0L;
        }
        if (key instanceof Double || key instanceof Float) {
            return ((Number) key).doubleValue();
        }
        if (key instanceof Number number) {
            return number.longValue();
        }
        return BinaryTermKeys.termBytes(key, format);
    }

    /** Resolves a term-source field's mapping, or null when the MapperService or mapping is unavailable. */
    private MappedFieldType resolveFieldType(String field) {
        MapperService mapperService = mapperServiceSupplier.get();
        return mapperService == null ? null : mapperService.fieldType(field);
    }

    /** Resolves a term-source field's mapping for rendering, failing loudly when it cannot be resolved. */
    private MappedFieldType requireFieldType(MultiTermsAggregationBuilder agg, String field) {
        MapperService mapperService = mapperServiceSupplier.get();
        if (mapperService == null) {
            throw new IllegalStateException(
                "index mapping unavailable for multi_terms aggregation ["
                    + agg.getName()
                    + "] — cannot resolve the key type for term source ["
                    + field
                    + "]"
            );
        }
        MappedFieldType fieldType = mapperService.fieldType(field);
        if (fieldType == null) {
            throw new IllegalStateException(
                "term source [" + field + "] of multi_terms aggregation [" + agg.getName() + "] is not present in the index mapping"
            );
        }
        return fieldType;
    }

    /** Bundles the request's bucket-count knobs for the result constructor. */
    private static TermsAggregator.BucketCountThresholds thresholds(MultiTermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize());
    }
}
