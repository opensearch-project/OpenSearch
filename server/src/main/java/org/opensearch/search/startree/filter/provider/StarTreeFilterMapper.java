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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.search.startree.DimensionOrdinalMapper;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.MatchNoneFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;

import java.io.IOException;
import java.util.ArrayList;
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
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.hasDecimalPart;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.signum;

public interface StarTreeFilterMapper {
    DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues);

    DimensionFilter getRangeMatchFilter(
        MappedFieldType mappedFieldType,
        Object rawLow,
        Object rawHigh,
        boolean includeLow,
        boolean includeHigh
    );

    Optional<Long> getMatchingOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues, DimensionOrdinalMapper.MatchType matchType);

    class Factory {

        private static final Map<NumberFieldMapper.NumberType, StarTreeFilterMapper> NUMERIC_SINGLETON_MAPPINGS = Map.of(
            BYTE,
            new IntegerFieldMapperNumeric(),
            SHORT,
            new IntegerFieldMapperNumeric(),
            INTEGER,
            new IntegerFieldMapperNumeric(),
            LONG,
            new SignedLongFieldMapperNumeric(),
            HALF_FLOAT,
            new HalfFloatFieldMapperNumeric(),
            FLOAT,
            new FloatFieldMapperNumeric(),
            DOUBLE,
            new DoubleFieldMapperNumeric()
        );

        public static StarTreeFilterMapper fromMappedFieldType(MappedFieldType mappedFieldType) {
            if (mappedFieldType instanceof KeywordFieldType) {
                return new KeywordFieldMapper();
            } else if (mappedFieldType instanceof NumberFieldType) {
                NumberFieldMapper.NumberType numberType = ((NumberFieldMapper.NumberFieldType) mappedFieldType).numberType();
                return NUMERIC_SINGLETON_MAPPINGS.get(numberType);
            } else {
                return null;
            }
        }
    }

}

abstract class NumericMapper implements StarTreeFilterMapper {

    @Override
    public Optional<Long> getMatchingOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues, DimensionOrdinalMapper.MatchType matchType) {
        // Casting to long ensures that all numeric fields have been converted to equivalent long at request parsing time.
        return Optional.of((long) value);
    }
}

abstract class NumericNonDecimalMapper extends NumericMapper {

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        if (mappedFieldType instanceof NumberFieldType) {
            NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;
            List<Object> convertedValues = new ArrayList<>(rawValues.size());
            for (Object rawValue : rawValues) {
                convertedValues.add(numberFieldType.numberType().parse(rawValue, true).longValue());
            }
            return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
        }
        return null;
    }

    @Override
    public DimensionFilter getRangeMatchFilter(
        MappedFieldType mappedFieldType,
        Object rawLow,
        Object rawHigh,
        boolean includeLow,
        boolean includeHigh
    ) {
        if (mappedFieldType instanceof NumberFieldType) {
            NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;

            Long parsedLow = rawLow == null ? defaultMinimum() : numberFieldType.numberType().parse(rawLow, true).longValue();
            Long parsedHigh = rawHigh == null ? defaultMaximum() : numberFieldType.numberType().parse(rawHigh, true).longValue();

            boolean lowerTermHasDecimalPart = hasDecimalPart(parsedLow);
            if ((lowerTermHasDecimalPart == false && includeLow == false) || (lowerTermHasDecimalPart && signum(parsedLow) > 0)) {
                if (parsedLow.equals(defaultMaximum())) {
                    return new MatchNoneFilter();
                }
                ++parsedLow;
            }
            boolean upperTermHasDecimalPart = hasDecimalPart(parsedHigh);
            if ((upperTermHasDecimalPart == false && includeHigh == false) || (upperTermHasDecimalPart && signum(parsedHigh) < 0)) {
                if (parsedHigh.equals(defaultMinimum())) {
                    return new MatchNoneFilter();
                }
                --parsedHigh;
            }
            return new RangeMatchDimFilter(mappedFieldType.name(), parsedLow, parsedHigh, true, true);
        }
        return null;
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

abstract class NumericDecimalFieldMapper extends NumericMapper {

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        if (mappedFieldType instanceof NumberFieldType) {
            NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;
            List<Object> convertedValues = new ArrayList<>(rawValues.size());
            for (Object rawValue : rawValues) {
                convertedValues.add(convertToDocValues(numberFieldType.numberType().parse(rawValue, true)));
            }
            return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
        }
        return null;
    }

    @Override
    public DimensionFilter getRangeMatchFilter(
        MappedFieldType mappedFieldType,
        Object rawLow,
        Object rawHigh,
        boolean includeLow,
        boolean includeHigh
    ) {
        if (mappedFieldType instanceof NumberFieldType) {
            NumberFieldType numberFieldType = (NumberFieldType) mappedFieldType;
            Number l = Long.MIN_VALUE;
            Number u = Long.MAX_VALUE;
            if (rawLow != null) {
                l = numberFieldType.numberType().parse(rawLow, false);
                if (includeLow == false) {
                    l = getNextHigh(l);
                }
                l = convertToDocValues(l);
            }
            if (rawHigh != null) {
                u = numberFieldType.numberType().parse(rawHigh, false);
                if (includeHigh == false) {
                    u = getNextLow(u);
                }
                u = convertToDocValues(u);
            }
            return new RangeMatchDimFilter(numberFieldType.name(), l, u, true, true);
        }
        return null;
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

class KeywordFieldMapper implements StarTreeFilterMapper {

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        if (mappedFieldType instanceof KeywordFieldType) {
            KeywordFieldType keywordFieldType = (KeywordFieldType) mappedFieldType;
            List<Object> convertedValues = new ArrayList<>(rawValues.size());
            for (Object rawValue : rawValues) {
                convertedValues.add(parseRawKeyword(mappedFieldType.name(), rawValue, keywordFieldType));
            }
            return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
        }
        return null;
    }

    @Override
    public DimensionFilter getRangeMatchFilter(
        MappedFieldType mappedFieldType,
        Object rawLow,
        Object rawHigh,
        boolean includeLow,
        boolean includeHigh
    ) {
        if (mappedFieldType instanceof KeywordFieldType) {
            KeywordFieldType keywordFieldType = (KeywordFieldType) mappedFieldType;
            return new RangeMatchDimFilter(
                mappedFieldType.name(),
                parseRawKeyword(mappedFieldType.name(), rawLow, keywordFieldType),
                parseRawKeyword(mappedFieldType.name(), rawHigh, keywordFieldType),
                includeLow,
                includeHigh
            );
        }
        return null;
    }

    @Override
    public Optional<Long> getMatchingOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues, DimensionOrdinalMapper.MatchType matchType) {
            StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
            if (genericIterator instanceof SortedSetStarTreeValuesIterator) {
                SortedSetStarTreeValuesIterator sortedSetIterator = (SortedSetStarTreeValuesIterator) genericIterator;
                try {
                    if (matchType == DimensionOrdinalMapper.MatchType.EXACT) {
                        return Optional.of(sortedSetIterator.lookupTerm((BytesRef) value));
                    } else {
                        TermsEnum termsEnum = sortedSetIterator.termsEnum();
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil((BytesRef) value);
                        if (matchType == DimensionOrdinalMapper.MatchType.GT || matchType == DimensionOrdinalMapper.MatchType.GTE) {
                            // We reached the end and couldn't match anything, else we found a term which matches.
                            return (seekStatus == TermsEnum.SeekStatus.END) ? Optional.empty() : Optional.of(termsEnum.ord());
                        } else { // LT || LTE
                            // If we found a term just greater, then return ordinal of the term just before it.
                            return Optional.of((seekStatus == TermsEnum.SeekStatus.NOT_FOUND) ? termsEnum.ord() - 1 : termsEnum.ord());
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported star tree values iterator " + genericIterator.getClass().getName());
            }
    }

    // TODO : Think around making TermBasedFT#indexedValueForSearch() accessor public for reuse here.
    private Object parseRawKeyword(String field, Object rawValue, KeywordFieldType keywordFieldType) {
        Object parsedValue;
        if (keywordFieldType.getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
            parsedValue = BytesRefs.toBytesRef(rawValue);
        } else {
            if (rawValue instanceof BytesRef) {
                rawValue = ((BytesRef) rawValue).utf8ToString();
            }
            parsedValue = keywordFieldType.getTextSearchInfo().getSearchAnalyzer().normalize(field, rawValue.toString());
        }
        return parsedValue;
    }

}
