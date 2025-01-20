/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.hasDecimalPart;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.signum;

@ExperimentalApi
public interface StarTreeFilterProvider {

    StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
        throws IOException;

    private static long parseRawNumberToDVLong(Object rawValue, NumberFieldMapper.NumberType numberType) {
        Object parsedValue = numberType.parse(rawValue, false);
        switch (numberType) {
            case BYTE:
            case SHORT:
            case INTEGER:
                return Long.parseLong(parsedValue.toString());
            case LONG:
                return (long) parsedValue;
            case HALF_FLOAT:
                return HalfFloatPoint.halfFloatToSortableShort((Float) parsedValue);
            case FLOAT:
                return NumericUtils.floatToSortableInt((Float) parsedValue);
            case DOUBLE:
                return NumericUtils.doubleToSortableLong((Double) parsedValue);
            default:
                throw new UnsupportedOperationException("Unsupported field type [" + numberType + "]");
        }
    }

    private static Object parseRawKeyword(String field, Object rawValue, KeywordFieldMapper.KeywordFieldType keywordFieldType) {
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

    class SingletonFactory {

        private static final Map<Class<? extends QueryBuilder>, StarTreeFilterProvider> QUERY_BUILDERS_TO_STF_PROVIDER = Map.of(
            TermQueryBuilder.class,
            new TermStarTreeFilterProvider(),
            TermsQueryBuilder.class,
            new TermsStarTreeFilterProvider(),
            RangeQueryBuilder.class,
            new RangeStarTreeFilterProvider()
        );

        public static StarTreeFilterProvider getProvider(QueryBuilder query) {
            if (query != null) {
                return QUERY_BUILDERS_TO_STF_PROVIDER.get(query.getClass());
            }
            return null; // QueryBuilder is not yet supported for filtering in star tree.
        }

    }

    class TermStarTreeFilterProvider implements StarTreeFilterProvider {
        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
            throws IOException {
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) rawFilter;
            String field = termQueryBuilder.fieldName();
            MappedFieldType mappedFieldType = context.mapperService().fieldType(field);
            Object term;
            Dimension matchedDimension = StarTreeQueryHelper.getMatchingDimensionOrNull(field, compositeFieldType.getDimensions());
            if (mappedFieldType.getClass().equals(NumberFieldMapper.NumberFieldType.class)) {
                Query query = termQueryBuilder.toQuery(context.getQueryShardContext());
                if (query instanceof MatchNoDocsQuery) {
                    return null; // Indicates Aggregators to fallback to default implementation.
                }
                NumberFieldMapper.NumberFieldType numFieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;
                term = parseRawNumberToDVLong(termQueryBuilder.value(), numFieldType.numberType());
            } else if (mappedFieldType.getClass().equals(KeywordFieldMapper.KeywordFieldType.class)) {
                KeywordFieldMapper.KeywordFieldType keywordFieldType = (KeywordFieldMapper.KeywordFieldType) mappedFieldType;
                term = parseRawKeyword(field, termQueryBuilder.value(), keywordFieldType);
            } else {
                return null;
            }
            // FIXME : DocValuesType validation is field type specific and not query builder specific should happen elsewhere.
            return matchedDimension == null
                ? null
                : new StarTreeFilter(Map.of(field, List.of(new ExactMatchDimFilter(field, List.of(term)))));
        }
    }

    class TermsStarTreeFilterProvider implements StarTreeFilterProvider {
        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
            throws IOException {
            TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) rawFilter;
            String field = termsQueryBuilder.fieldName();
            Dimension matchedDimension = StarTreeQueryHelper.getMatchingDimensionOrNull(field, compositeFieldType.getDimensions());
            if (matchedDimension == null) {
                return null; // Indicates Aggregators to fallback to default implementation.
            } else {
                MappedFieldType mappedFieldType = context.mapperService().fieldType(field);
                Query query = termsQueryBuilder.toQuery(context.getQueryShardContext());
                if (query instanceof MatchNoDocsQuery) {
                    return new StarTreeFilter(Collections.emptyMap());
                } else {
                    List<Object> convertedValues = new ArrayList<>(termsQueryBuilder.values().size());
                    if (mappedFieldType.getClass().equals(NumberFieldMapper.NumberFieldType.class)) {
                        NumberFieldMapper.NumberFieldType numFieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;
                        for (Object rawValue : termsQueryBuilder.values()) {
                            convertedValues.add(parseRawNumberToDVLong(rawValue, numFieldType.numberType()));
                        }
                    } else if (mappedFieldType.getClass().equals(KeywordFieldMapper.KeywordFieldType.class)) {
                        KeywordFieldMapper.KeywordFieldType keywordFieldType = (KeywordFieldMapper.KeywordFieldType) mappedFieldType;
                        for (Object rawValue : termsQueryBuilder.values()) {
                            convertedValues.add(parseRawKeyword(field, rawValue, keywordFieldType));
                        }
                    } else {
                        return null; // Fallback to default implementation as mapped type is not yet supported.
                    }
                    return new StarTreeFilter(Map.of(field, List.of(new ExactMatchDimFilter(field, convertedValues))));
                }
            }
        }
    }

    class RangeStarTreeFilterProvider implements StarTreeFilterProvider {

        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
            throws IOException {
            RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) rawFilter;
            String field = rangeQueryBuilder.fieldName();
            Dimension matchedDimension = StarTreeQueryHelper.getMatchingDimensionOrNull(field, compositeFieldType.getDimensions());
            if (matchedDimension == null) {
                return null;
            }
            MappedFieldType mappedFieldType = context.mapperService().fieldType(field);
            Query query = rangeQueryBuilder.toQuery(context.getQueryShardContext());
            if (query instanceof MatchNoDocsQuery) {
                return new StarTreeFilter(Collections.emptyMap());
            } else {
                if (mappedFieldType.getClass().equals(NumberFieldMapper.NumberFieldType.class)) {
                    NumberFieldMapper.NumberFieldType numFieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;
                    DimensionFilter dimensionFilter;
                    switch (numFieldType.numberType()) {
                        case BYTE:
                        case SHORT:
                        case INTEGER:
                        case LONG:
                            dimensionFilter = getRangeFilterForNonDecimals(rangeQueryBuilder, numFieldType.numberType());
                            break;
                        case HALF_FLOAT:
                        case FLOAT:
                        case DOUBLE:
                            dimensionFilter = getRangeFilterForDecimals(rangeQueryBuilder, numFieldType.numberType());
                            break;
                        default:
                            return null;
                    }
                    return new StarTreeFilter(Map.of(field, List.of(dimensionFilter)));
                } else if (mappedFieldType.getClass().equals(KeywordFieldMapper.KeywordFieldType.class)) {
                    KeywordFieldMapper.KeywordFieldType keywordFieldType = (KeywordFieldMapper.KeywordFieldType) mappedFieldType;
                    return new StarTreeFilter(
                        Map.of(
                            field,
                            List.of(
                                new RangeMatchDimFilter(
                                    field,
                                    parseRawKeyword(field, rangeQueryBuilder.from(), keywordFieldType),
                                    parseRawKeyword(field, rangeQueryBuilder.to(), keywordFieldType),
                                    rangeQueryBuilder.includeLower(),
                                    rangeQueryBuilder.includeUpper()
                                )
                            )
                        )
                    );
                }
                return null;
            }
        }

        private static DimensionFilter getRangeFilterForNonDecimals(
            RangeQueryBuilder rangeQueryBuilder,
            NumberFieldMapper.NumberType numFieldType
        ) {
            Long low = rangeQueryBuilder.from() == null ? Long.MIN_VALUE : parseRawNumberToDVLong(rangeQueryBuilder.from(), numFieldType);
            Long high = rangeQueryBuilder.to() == null ? Long.MAX_VALUE : parseRawNumberToDVLong(rangeQueryBuilder.to(), numFieldType);
            boolean lowerTermHasDecimalPart = hasDecimalPart(low);
            if ((lowerTermHasDecimalPart == false && rangeQueryBuilder.includeLower() == false)
                || (lowerTermHasDecimalPart && signum(low) > 0)) {
                if (low.equals(Long.MAX_VALUE)) {
                    return new MatchNoneFilter();
                }
                ++low;
            }
            boolean upperTermHasDecimalPart = hasDecimalPart(high);
            if ((upperTermHasDecimalPart == false && rangeQueryBuilder.includeUpper() == false)
                || (upperTermHasDecimalPart && signum(high) < 0)) {
                if (high.equals(Long.MIN_VALUE)) {
                    return new MatchNoneFilter();
                }
                --high;
            }
            return new RangeMatchDimFilter(rangeQueryBuilder.fieldName(), low, high, true, true);
        }

        private DimensionFilter getRangeFilterForDecimals(RangeQueryBuilder rangeQueryBuilder, NumberFieldMapper.NumberType numFieldType) {
            Number l = Long.MIN_VALUE;
            Number u = Long.MAX_VALUE;
            if (rangeQueryBuilder.from() != null) {
                l = numFieldType.parse(rangeQueryBuilder.from(), false);
                if (rangeQueryBuilder.includeLower() == false) {
                    l = getNextHighOrLowForDecimal(numFieldType, l, true);
                }
                l = parseRawNumberToDVLong(l, numFieldType);
            }
            if (rangeQueryBuilder.to() != null) {
                u = numFieldType.parse(rangeQueryBuilder.to(), false);
                if (rangeQueryBuilder.includeUpper() == false) {
                    u = getNextHighOrLowForDecimal(numFieldType, u, false);
                }
                u = parseRawNumberToDVLong(u, numFieldType);
            }
            return new RangeMatchDimFilter(
                rangeQueryBuilder.fieldName(),
                l,
                u,
                rangeQueryBuilder.includeLower(),
                rangeQueryBuilder.includeUpper()
            );
        }

        private static Number getNextHighOrLowForDecimal(
            NumberFieldMapper.NumberType numFieldType,
            Number value,
            boolean returnNextHighest
        ) {
            switch (numFieldType) {
                case HALF_FLOAT:
                    return returnNextHighest ? HalfFloatPoint.nextUp((Float) value) : HalfFloatPoint.nextDown((Float) value);
                case FLOAT:
                    return returnNextHighest ? FloatPoint.nextUp((Float) value) : FloatPoint.nextDown((Float) value);
                case DOUBLE:
                    return returnNextHighest ? DoublePoint.nextUp((Double) value) : DoublePoint.nextDown((Double) value);
                default:
                    throw new IllegalArgumentException("Invalid field type [" + numFieldType + "] for decimal");
            }
        }
    }

}
