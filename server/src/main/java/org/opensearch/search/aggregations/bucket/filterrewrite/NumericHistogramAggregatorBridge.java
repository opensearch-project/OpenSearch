/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * For numeric histogram aggregation
 * <p>
 * A histogram bucket is the half open interval of values that round to the same key, so the whole aggregation
 * is a set of adjacent ranges and can be answered by walking the BKD tree instead of reading the doc values of
 * every matching document.
 * <p>
 * The doc values path buckets a value with {@code floor((value - offset) / interval)} in double arithmetic.
 * The ranges built here have to agree with that on every value the field can hold, which double rounding does
 * not give for free, so each bucket boundary is snapped to a value the field can actually hold and then checked
 * against that formula. A boundary that does not check out turns the optimization off for the whole aggregation
 * rather than producing counts that differ from the doc values path.
 */
public abstract class NumericHistogramAggregatorBridge extends AggregatorBridge {

    private NumberFieldMapper.NumberFieldType numberFieldType;
    private double interval;
    private double offset;
    private int maxRewriteFilters;

    protected boolean canOptimize(ValuesSourceConfig config, double interval, double offset, DoubleBounds hardBounds) {
        // Runtime path: FilterRewriteOptimizationContext#canOptimize already gates on parent == null before
        // reaching here, so parent is passed as null.
        if (filterRewriteFastPathApplies(null, config, interval, offset, hardBounds) == false) {
            return false;
        }
        this.fieldType = config.fieldType();
        this.numberFieldType = (NumberFieldMapper.NumberFieldType) this.fieldType.unwrap();
        this.interval = interval;
        this.offset = offset;
        return true;
    }

    /**
     * Whether the numeric histogram filter-rewrite fast path applies for this aggregation. The single source of
     * truth for the fast-path preconditions -- top-level only ({@code parent == null}), a searchable numeric
     * field, no script/missing, no hard bounds, and a finite interval and offset. Used both at runtime
     * ({@link #canOptimize}) and by the histogram aggregator factory to decide intra-segment eligibility:
     * intra-segment search is used only when this returns false (the fast path is unavailable and the
     * doc-by-doc fallback, which parallelizes, runs).
     */
    public static boolean filterRewriteFastPathApplies(
        Object parent,
        ValuesSourceConfig config,
        double interval,
        double offset,
        DoubleBounds hardBounds
    ) {
        // The fast path (BKD point-tree precompute) only runs for a top-level agg; nested aggs collect
        // doc-by-doc under their parent's buckets.
        if (parent != null) {
            return false;
        }
        // The doc values path tests hard bounds against `key * interval`, which leaves `offset` out. Rather
        // than carry that over to a second code path, leave hard bounds on the doc values path.
        if (hardBounds != null) {
            return false;
        }
        if (interval <= 0 || Double.isFinite(interval) == false || Double.isFinite(offset) == false) {
            return false;
        }
        if (config.script() != null || config.missing() != null) {
            return false;
        }
        if ((config.getValuesSource() instanceof ValuesSource.Numeric.FieldData) == false) {
            return false;
        }
        MappedFieldType fieldType = config.fieldType();
        if (fieldType == null || fieldType.isSearchable() == false) {
            return false;
        }
        MappedFieldType unwrapped = fieldType.unwrap();
        if ((unwrapped instanceof NumberFieldMapper.NumberFieldType) == false) {
            // Dates and booleans are served by this aggregator too, but neither encodes its points the way the
            // ranges below are built.
            return false;
        }
        // Unsigned longs are stored as 16 byte big integers and reach the doc values path through a different
        // conversion; left out of the first cut.
        return ((NumberFieldMapper.NumberFieldType) unwrapped).numberType() != NumberFieldMapper.NumberType.UNSIGNED_LONG;
    }

    protected void buildRanges(SearchContext context) throws IOException {
        this.maxRewriteFilters = context.maxAggRewriteFilters();
        setRanges.accept(buildRanges(Helper.getNumericHistoAggBounds(context, numberFieldType)));
    }

    @Override
    final Ranges tryBuildRangesFromSegment(LeafReaderContext leaf) throws IOException {
        return buildRanges(Helper.getNumericSegmentBounds(leaf, numberFieldType));
    }

    /**
     * Turns the min and max of the values being aggregated into one range per histogram bucket.
     *
     * @param bounds the inclusive low and high value of the aggregation, or null when they are not known
     * @return null when the aggregation cannot be answered from the ranges, in which case the caller falls
     *         back to the doc values path
     */
    private Ranges buildRanges(double[] bounds) {
        if (bounds == null) {
            return null;
        }
        final double low = bounds[0];
        final double high = bounds[1];
        if (Double.isFinite(low) == false || Double.isFinite(high) == false || low > high) {
            return null;
        }

        final double firstKey = bucketKey(low);
        final double lastKey = bucketKey(high);
        if (Double.isFinite(firstKey) == false || Double.isFinite(lastKey) == false) {
            return null;
        }

        // Counted as a double first: with a small interval over a wide range the bucket count overflows an int.
        final double bucketCountAsDouble = lastKey - firstKey + 1;
        if (bucketCountAsDouble < 1 || bucketCountAsDouble > maxRewriteFilters) {
            logger.debug(
                "Bucket count [{}] over the limit of [{}], skip the fast filter optimization",
                bucketCountAsDouble,
                maxRewriteFilters
            );
            return null;
        }
        final int bucketCount = (int) bucketCountAsDouble;

        final byte[][] lowers = new byte[bucketCount][];
        final byte[][] uppers = new byte[bucketCount][];

        // The first bucket starts at the lowest value there is rather than at its own key, and the last one
        // ends just past the highest, which keeps every encoded boundary inside the range the field can hold.
        lowers[0] = encodeExactly(low);
        if (lowers[0] == null) {
            return null;
        }

        for (int i = 1; i < bucketCount; i++) {
            final double key = firstKey + i;
            final byte[] boundary = snapUp(key * interval + offset);
            if (boundary == null) {
                return null;
            }
            // The boundary has to land where the doc values path puts it: the value itself in this bucket, and
            // the value below it in the previous one. Everything between two checked boundaries then follows,
            // because the bucket key only ever grows with the value.
            if (bucketKey(decode(boundary)) != key || checkValueBelow(boundary, key) == false) {
                logger.debug(
                    "Bucket boundary for key [{}] is not exact on field [{}], skip the fast filter optimization",
                    key,
                    fieldType.name()
                );
                return null;
            }
            uppers[i - 1] = boundary;
            lowers[i] = boundary;
        }

        final byte[] highEncoded = encodeExactly(high);
        if (highEncoded == null) {
            return null;
        }
        uppers[bucketCount - 1] = nextPointValue(highEncoded);
        if (uppers[bucketCount - 1] == null) {
            return null;
        }

        return new Ranges(lowers, uppers);
    }

    /**
     * Whether the value right below a bucket boundary rounds into the bucket before it, which is what makes
     * the boundary the exact point the doc values path switches buckets at.
     * <p>
     * A boundary of zero is the one place this cannot be asked: the values just below zero are subnormal, and
     * dividing one by the interval underflows back to negative zero, so the doc values path rounds it into the
     * zero bucket rather than the one before it. Those values are counted one bucket lower here. The band this
     * covers is narrower than {@code interval * Double.MIN_VALUE}, and the doc values path puts them in a
     * bucket keyed negative zero that is already separate from the zero bucket, so there is no sensible
     * behaviour to preserve -- and giving up on every histogram whose buckets straddle zero would cost far
     * more than it is worth.
     */
    private boolean checkValueBelow(byte[] boundary, double key) {
        if (decode(boundary) == 0.0) {
            return true;
        }
        final byte[] below = previousPointValue(boundary);
        return below != null && bucketKey(decode(below)) == key - 1;
    }

    /**
     * The bucket key of a value, computed exactly the way the doc values path computes it
     */
    private double bucketKey(double value) {
        return Math.floor((value - offset) / interval);
    }

    private double decode(byte[] encoded) {
        return numberFieldType.parsePoint(encoded).doubleValue();
    }

    /**
     * Encodes a value, and returns null unless the encoding holds it exactly. {@code encodePoint} narrows --
     * truncating for integral types, rounding for floating point ones -- so a range built on a value it could
     * not hold would not line up with the values in the index.
     */
    private byte[] encodeExactly(double value) {
        final byte[] encoded = numberFieldType.encodePoint(value);
        return decode(encoded) == value ? encoded : null;
    }

    /**
     * The smallest value the field can hold that is not below {@code value}, encoded.
     * <p>
     * {@code encodePoint} lands on an adjacent value at worst -- it truncates towards zero for integral types
     * and rounds to nearest for floating point ones -- so one step up is enough to cover it.
     */
    private byte[] snapUp(double value) {
        byte[] encoded = numberFieldType.encodePoint(value);
        if (decode(encoded) < value) {
            encoded = nextPointValue(encoded);
            if (encoded == null || decode(encoded) < value) {
                return null;
            }
        }
        // Negative zero encodes below positive zero while comparing equal to it, so a boundary of zero has to
        // be taken down to it -- otherwise a document holding -0.0 would be counted in the bucket below the one
        // it rounds into. Zero is the only value two point encodings share, so one step covers it.
        final byte[] previous = previousPointValue(encoded);
        if (previous != null && decode(previous) >= value) {
            encoded = previous;
        }
        return encoded;
    }

    /**
     * The next value the field can hold above an encoded one.
     * <p>
     * Point encodings sort in unsigned byte order, so stepping through them is an increment of the encoded
     * bytes. Doing it here rather than in value space means not having to know how far apart two adjacent
     * values of this field are, or to watch for the arithmetic overflowing its type.
     *
     * @return null if the value is already the highest the encoding can hold
     */
    private static byte[] nextPointValue(byte[] encoded) {
        final byte[] next = encoded.clone();
        for (int i = next.length - 1; i >= 0; i--) {
            if (++next[i] != 0) {
                return next;
            }
        }
        return null;
    }

    /**
     * The previous value the field can hold below an encoded one, the counterpart of {@link #nextPointValue}
     *
     * @return null if the value is already the lowest the encoding can hold
     */
    private static byte[] previousPointValue(byte[] encoded) {
        final byte[] previous = encoded.clone();
        for (int i = previous.length - 1; i >= 0; i--) {
            if (previous[i]-- != 0) {
                return previous;
            }
        }
        return null;
    }

    @Override
    final FilterRewriteOptimizationContext.OptimizeResult tryOptimize(
        PointValues values,
        BiConsumer<Long, Long> incrementDocCount,
        Ranges ranges,
        FilterRewriteOptimizationContext.SubAggCollectorParam subAggCollectorParam
    ) throws IOException {
        final Function<Integer, Long> getBucketOrd = (activeIndex) -> {
            // Every lower bound was checked to round to its own bucket key while the ranges were built. The
            // bucket at zero comes back as negative zero, which keys a bucket of its own next to the positive
            // zero a segment left on the doc values path would produce; adding zero folds the two together.
            final double key = bucketKey(decode(ranges.getLowers()[activeIndex])) + 0.0;
            final long bucketOrd = bucketOrdProducer().apply(key);
            return bucketOrd < 0 ? -1 - bucketOrd : bucketOrd; // already seen
        };

        return getResult(values, incrementDocCount, ranges, getBucketOrd, Integer.MAX_VALUE, subAggCollectorParam);
    }

    /**
     * Provides a function to produce bucket ordinals from the key of the corresponding bucket
     */
    protected abstract Function<Double, Long> bucketOrdProducer();
}
