/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Numbers;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.compositeindex.datacube.DimensionDataType;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.MatchNoneFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.BYTE;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.DOUBLE;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.FLOAT;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.HALF_FLOAT;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.INTEGER;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.LONG;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.SHORT;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.UNSIGNED_LONG;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.hasDecimalPart;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.signum;

/**
 * Generates the @{@link DimensionFilter} raw values and the @{@link MappedFieldType} of the dimension.
 */
@ExperimentalApi
public interface DimensionFilterMapper {
    /**
     * Generates @{@link ExactMatchDimFilter} from Term/Terms query input.
     * @param mappedFieldType:
     * @param rawValues:
     * @return :
     */
    DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues);

    /**
     * Generates @{@link RangeMatchDimFilter} from Range query input.
     * @param mappedFieldType:
     * @param rangeQuery:
     * @return :
     */
    DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, StarTreeRangeQuery rangeQuery);

    /**
     * Called during conversion from parsedUserInput to segmentOrdinal for every segment.
     * @param dimensionName:
     * @param value:
     * @param starTreeValues:
     * @param matchType:
     * @return :
     */
    Optional<Long> getMatchingOrdinal(
        String dimensionName,
        Object value,
        StarTreeValues starTreeValues,
        DimensionFilter.MatchType matchType
    );

    /**
     * Compares two values of the same type.
     * @param v1 first object
     * @param v2 second object
     * @return :
     */
    int compareValues(Object v1, Object v2);

    /**
     * Checks if a value falls within a range.
     * Default implementation for regular types.
     */
    default boolean isValueInRange(Object value, Object low, Object high, boolean includeLow, boolean includeHigh) {
        if (low != null) {
            int comparison = compareValues(value, low);
            if (comparison < 0 || (comparison == 0 && !includeLow)) {
                return false;
            }
        }

        if (high != null) {
            int comparison = compareValues(value, high);
            if (comparison > 0 || (comparison == 0 && !includeHigh)) {
                return false;
            }
        }
        return true;
    }

    default boolean isValidRange(Object low, Object high, boolean includeLow, boolean includeHigh) {
        if (low == null || high == null) {
            return true;
        }
        int comparison = compareValues(low, high);
        return comparison < 0 || (comparison == 0 && includeLow && includeHigh);
    }

    default Comparator<Long> comparator() {
        return DimensionDataType.LONG::compare;
    }

    default boolean resolveUsingSubDimension() {
        return false;
    }

    default String getSubDimensionFieldEffective(String subDimensionField1, String subDimensionField2) {
        return null;
    }

    default List<DimensionFilter> getFinalDimensionFilters(List<DimensionFilter> filters) {
        return filters;
    }

    /**
     * Singleton Factory for @{@link DimensionFilterMapper}
     */
    class Factory {

        private static final Map<String, DimensionFilterMapper> DIMENSION_FILTER_MAPPINGS = Map.of(
            BYTE.typeName(),
            new IntegerFieldMapperNumeric(),
            SHORT.typeName(),
            new IntegerFieldMapperNumeric(),
            INTEGER.typeName(),
            new IntegerFieldMapperNumeric(),
            LONG.typeName(),
            new SignedLongFieldMapperNumeric(),
            HALF_FLOAT.typeName(),
            new HalfFloatFieldMapperNumeric(),
            FLOAT.typeName(),
            new FloatFieldMapperNumeric(),
            DOUBLE.typeName(),
            new DoubleFieldMapperNumeric(),
            org.opensearch.index.mapper.KeywordFieldMapper.CONTENT_TYPE,
            new KeywordFieldMapper(),
            UNSIGNED_LONG.typeName(),
            new UnsignedLongFieldMapperNumeric(),
            org.opensearch.index.mapper.IpFieldMapper.CONTENT_TYPE,
            new IpFieldMapper()
        );

        public static DimensionFilterMapper fromMappedFieldType(MappedFieldType mappedFieldType, SearchContext searchContext) {
            if (mappedFieldType != null) {
                if (DateFieldMapper.CONTENT_TYPE.equals(mappedFieldType.typeName())) {
                    return new StarDateFieldMapper(searchContext);
                }
                return DIMENSION_FILTER_MAPPINGS.get(mappedFieldType.typeName());
            }
            return null;
        }
    }

}

abstract class NumericMapper implements DimensionFilterMapper {

    @Override
    public Optional<Long> getMatchingOrdinal(
        String dimensionName,
        Object value,
        StarTreeValues starTreeValues,
        DimensionFilter.MatchType matchType
    ) {
        // Casting to long ensures that all numeric fields have been converted to equivalent long at request parsing time.
        return Optional.of((long) value);
    }

    @Override
    public int compareValues(Object v1, Object v2) {
        if (!(v1 instanceof Long) || !(v2 instanceof Long)) {
            throw new IllegalArgumentException("Expected Long values for numeric comparison");
        }
        return Long.compare((Long) v1, (Long) v2);
    }
}

abstract class NumericNonDecimalMapper extends NumericMapper {

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;
        List<Object> convertedValues = new ArrayList<>(rawValues.size());
        for (Object rawValue : rawValues) {
            convertedValues.add(numberFieldType.numberType().parse(rawValue, true).longValue());
        }
        return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
    }

    @Override
    public DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, StarTreeRangeQuery rangeQuery) {
        NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;

        Long parsedLow = rangeQuery.from() == null
            ? defaultMinimum()
            : numberFieldType.numberType().parse(rangeQuery.from(), true).longValue();
        Long parsedHigh = rangeQuery.to() == null
            ? defaultMaximum()
            : numberFieldType.numberType().parse(rangeQuery.to(), true).longValue();

        boolean lowerTermHasDecimalPart = hasDecimalPart(parsedLow);
        if ((lowerTermHasDecimalPart == false && rangeQuery.includeLower() == false)
            || (lowerTermHasDecimalPart && signum(parsedLow) > 0)) {
            if (parsedLow.equals(defaultMaximum())) {
                return new MatchNoneFilter();
            }
            ++parsedLow;
        }
        boolean upperTermHasDecimalPart = hasDecimalPart(parsedHigh);
        if ((upperTermHasDecimalPart == false && rangeQuery.includeUpper() == false)
            || (upperTermHasDecimalPart && signum(parsedHigh) < 0)) {
            if (parsedHigh.equals(defaultMinimum())) {
                return new MatchNoneFilter();
            }
            --parsedHigh;
        }
        return new RangeMatchDimFilter(mappedFieldType.name(), parsedLow, parsedHigh, true, true);
    }

    abstract Long defaultMinimum();

    abstract Long defaultMaximum();

}

class IntegerFieldMapperNumeric extends NumericNonDecimalMapper {
    @Override
    Long defaultMinimum() {
        return (long) Integer.MIN_VALUE;
    }

    @Override
    Long defaultMaximum() {
        return (long) Integer.MAX_VALUE;
    }
}

class SignedLongFieldMapperNumeric extends NumericNonDecimalMapper {
    @Override
    Long defaultMinimum() {
        return Long.MIN_VALUE;
    }

    @Override
    Long defaultMaximum() {
        return Long.MAX_VALUE;
    }
}

class UnsignedLongFieldMapperNumeric extends NumericNonDecimalMapper {

    @Override
    Long defaultMinimum() {
        return Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG;
    }

    @Override
    Long defaultMaximum() {
        return Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG;
    }

    @Override
    public Comparator<Long> comparator() {
        return DimensionDataType.UNSIGNED_LONG::compare;
    }

    @Override
    public int compareValues(Object v1, Object v2) {
        if (!(v1 instanceof Long) || !(v2 instanceof Long)) {
            throw new IllegalArgumentException("Expected Long values for unsigned comparison");
        }
        return Long.compareUnsigned((Long) v1, (Long) v2);
    }

    @Override
    public boolean isValueInRange(Object value, Object low, Object high, boolean includeLow, boolean includeHigh) {
        long v = (Long) value;
        long l = low != null ? (Long) low : 0L;
        long h = high != null ? (Long) high : -1L; // -1L is max unsigned

        if (Long.compareUnsigned(l, h) > 0) {
            return (Long.compareUnsigned(v, l) > 0 || (Long.compareUnsigned(v, l) == 0 && includeLow))
                || (Long.compareUnsigned(v, h) < 0 || (Long.compareUnsigned(v, h) == 0 && includeHigh));
        }

        // Normal case
        return super.isValueInRange(value, low, high, includeLow, includeHigh);
    }

}

abstract class NumericDecimalFieldMapper extends NumericMapper {

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;
        List<Object> convertedValues = new ArrayList<>(rawValues.size());
        for (Object rawValue : rawValues) {
            convertedValues.add(convertToDocValues(numberFieldType.numberType().parse(rawValue, true)));
        }
        return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
    }

    @Override
    public DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, StarTreeRangeQuery rangeQuery) {
        NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;
        Number l = Long.MIN_VALUE;
        Number u = Long.MAX_VALUE;
        if (rangeQuery.from() != null) {
            l = numberFieldType.numberType().parse(rangeQuery.from(), false);
            if (rangeQuery.includeLower() == false) {
                l = getNextHigh(l);
            }
            l = convertToDocValues(l);
        }
        if (rangeQuery.to() != null) {
            u = numberFieldType.numberType().parse(rangeQuery.to(), false);
            if (rangeQuery.includeUpper() == false) {
                u = getNextLow(u);
            }
            u = convertToDocValues(u);
        }
        return new RangeMatchDimFilter(numberFieldType.name(), l, u, true, true);
    }

    abstract long convertToDocValues(Number parsedValue);

    abstract Number getNextLow(Number parsedValue);

    abstract Number getNextHigh(Number parsedValue);

}

class HalfFloatFieldMapperNumeric extends NumericDecimalFieldMapper {
    @Override
    long convertToDocValues(Number parsedValue) {
        return HalfFloatPoint.halfFloatToSortableShort((Float) parsedValue);
    }

    @Override
    Number getNextLow(Number parsedValue) {
        return HalfFloatPoint.nextDown((Float) parsedValue);
    }

    @Override
    Number getNextHigh(Number parsedValue) {
        return HalfFloatPoint.nextUp((Float) parsedValue);
    }
}

class FloatFieldMapperNumeric extends NumericDecimalFieldMapper {
    @Override
    long convertToDocValues(Number parsedValue) {
        return NumericUtils.floatToSortableInt((Float) parsedValue);
    }

    @Override
    Number getNextLow(Number parsedValue) {
        return FloatPoint.nextDown((Float) parsedValue);
    }

    @Override
    Number getNextHigh(Number parsedValue) {
        return FloatPoint.nextUp((Float) parsedValue);
    }
}

class DoubleFieldMapperNumeric extends NumericDecimalFieldMapper {
    @Override
    long convertToDocValues(Number parsedValue) {
        return NumericUtils.doubleToSortableLong((Double) parsedValue);
    }

    @Override
    Number getNextLow(Number parsedValue) {
        return DoublePoint.nextDown((Double) parsedValue);
    }

    @Override
    Number getNextHigh(Number parsedValue) {
        return DoublePoint.nextUp((Double) parsedValue);
    }
}

abstract class OrdinalFieldMapper implements DimensionFilterMapper {

    abstract Object parseRawField(String field, Object rawValue, MappedFieldType mappedFieldType) throws IllegalArgumentException;

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        List<Object> convertedValues = new ArrayList<>(rawValues.size());
        for (Object rawValue : rawValues) {
            convertedValues.add(parseRawField(mappedFieldType.name(), rawValue, mappedFieldType));
        }
        return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
    }

    @Override
    public DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, StarTreeRangeQuery rangeQuery) {
        return new RangeMatchDimFilter(
            mappedFieldType.name(),
            parseRawField(mappedFieldType.name(), rangeQuery.from(), mappedFieldType),
            parseRawField(mappedFieldType.name(), rangeQuery.to(), mappedFieldType),
            rangeQuery.includeLower(),
            rangeQuery.includeUpper()
        );
    }

    @Override
    public Optional<Long> getMatchingOrdinal(
        String dimensionName,
        Object value,
        StarTreeValues starTreeValues,
        DimensionFilter.MatchType matchType
    ) {
        SortedSetStarTreeValuesIterator sortedSetIterator = (SortedSetStarTreeValuesIterator) starTreeValues.getDimensionValuesIterator(
            dimensionName
        );
        try {
            if (matchType == DimensionFilter.MatchType.EXACT) {
                long ordinal = sortedSetIterator.lookupTerm((BytesRef) value);
                return ordinal >= 0 ? Optional.of(ordinal) : Optional.empty();
            } else {
                TermsEnum termsEnum = sortedSetIterator.termsEnum();
                TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil((BytesRef) value);
                // We reached the end and couldn't match anything, else we found a term which matches.
                // LT || LTE
                // If we found a term just greater, then return ordinal of the term just before it.
                // Checking if we are in bounds for satisfying LT
                // Checking if we are in bounds for satisfying LT
                switch (matchType) {
                    case GTE:
                        return seekStatus == TermsEnum.SeekStatus.END ? Optional.empty() : Optional.of(termsEnum.ord());
                    case GT:
                        return switch (seekStatus) {
                            case END -> Optional.empty();
                            case FOUND -> ((termsEnum.ord() + 1) < sortedSetIterator.getValueCount())
                                ? Optional.of(termsEnum.ord() + 1)
                                : Optional.empty();
                            case NOT_FOUND -> Optional.of(termsEnum.ord());
                        };
                    case LTE:
                        if (seekStatus == TermsEnum.SeekStatus.NOT_FOUND) {
                            return ((termsEnum.ord() - 1) >= 0) ? Optional.of(termsEnum.ord() - 1) : Optional.empty();
                        } else {
                            return Optional.of(termsEnum.ord());
                        }
                    case LT:
                        if (seekStatus == TermsEnum.SeekStatus.END) {
                            return Optional.of(termsEnum.ord());
                        } else {
                            return ((termsEnum.ord() - 1) >= 0) ? Optional.of(termsEnum.ord() - 1) : Optional.empty();
                        }
                    default:
                        throw new IllegalStateException("unexpected matchType " + matchType);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareValues(Object v1, Object v2) {
        if (!(v1 instanceof BytesRef) || !(v2 instanceof BytesRef)) {
            throw new IllegalArgumentException("Expected BytesRef values for comparison");
        }
        return ((BytesRef) v1).compareTo((BytesRef) v2);
    }
}

class KeywordFieldMapper extends OrdinalFieldMapper {

    // TODO : Think around making TermBasedFT#indexedValueForSearch() accessor public for reuse here.
    Object parseRawField(String field, Object rawValue, MappedFieldType mappedFieldType) {
        KeywordFieldType keywordFieldType = (KeywordFieldType) mappedFieldType;
        Object parsedValue = null;
        if (rawValue != null) {
            if (keywordFieldType.getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                parsedValue = BytesRefs.toBytesRef(rawValue);
            } else {
                if (rawValue instanceof BytesRef) {
                    rawValue = ((BytesRef) rawValue).utf8ToString();
                }
                parsedValue = keywordFieldType.getTextSearchInfo().getSearchAnalyzer().normalize(field, rawValue.toString());
            }
        }
        return parsedValue;
    }
}

/**
 * This class provides functionality to map IP address values for exact and range-based
 * filtering within a Star-Tree index. It handles the conversion of IP address
 * objects into sortable {@link BytesRef}.
 */
class IpFieldMapper extends OrdinalFieldMapper {

    /**
     * Parses a raw IP address value into a sortable {@link BytesRef}.
     *
     * This method handles various input types, including {@link InetAddress}, {@link BytesRef},
     * and {@link String}, converting them into a binary representation using
     * {@link InetAddressPoint#encode(InetAddress)}.
     *
     * @param field The name of the field being processed.
     * @param rawValue The raw IP address value.
     * @return A {@link BytesRef} representation of the IP address, or null if the input is null.
     */
    Object parseRawField(String field, Object rawValue, MappedFieldType mappedFieldType) throws IllegalArgumentException {
        Object parsedValue = null;
        if (rawValue != null) {
            try {
                switch (rawValue) {
                    case InetAddress inetAddress -> {
                        parsedValue = new BytesRef(InetAddressPoint.encode(inetAddress));
                    }
                    case BytesRef bytesRef -> {
                        return bytesRef;
                    }
                    case String s -> {
                        InetAddress addr = InetAddress.getByName(s);
                        parsedValue = new BytesRef(InetAddressPoint.encode(addr));
                    }
                    default -> {
                        throw new IllegalArgumentException(
                            "Unsupported value type for IP field [" + field + "]: " + rawValue.getClass().getName()
                        );
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse IP value for field [" + field + "]", e);
            }
        }
        return parsedValue;
    }
}
