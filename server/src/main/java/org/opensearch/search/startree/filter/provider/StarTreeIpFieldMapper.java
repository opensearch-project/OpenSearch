/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link DimensionFilterMapper} for IP address fields.
 *
 * This class provides functionality to map IP address values for exact and range-based
 * filtering within a Star-Tree index. It handles the conversion of IP address
 * objects into a sortable binary format (BytesRef) suitable for Lucene's term-based
 * data structures.
 */
class StarTreeIpFieldMapper implements DimensionFilterMapper {

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        List<Object> convertedValues = new ArrayList<>(rawValues.size());
        for (Object rawValue : rawValues) {
            convertedValues.add(parseRawIp(mappedFieldType.name(), rawValue));
        }
        return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);
    }

    @Override
    public DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, StarTreeRangeQuery rangeQuery) {
        return new RangeMatchDimFilter(
            mappedFieldType.name(),
            parseRawIp(mappedFieldType.name(), rangeQuery.from()),
            parseRawIp(mappedFieldType.name(), rangeQuery.to()),
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

                return switch (matchType) {
                    case GTE -> seekStatus == TermsEnum.SeekStatus.END ? Optional.empty() : Optional.of(termsEnum.ord());
                    case GT -> switch (seekStatus) {
                        case END -> Optional.empty();
                        case FOUND -> ((termsEnum.ord() + 1) < sortedSetIterator.getValueCount())
                            ? Optional.of(termsEnum.ord() + 1)
                            : Optional.empty();
                        case NOT_FOUND -> Optional.of(termsEnum.ord());
                    };
                    case LTE -> {
                        if (seekStatus == TermsEnum.SeekStatus.NOT_FOUND) {
                            yield ((termsEnum.ord() - 1) >= 0) ? Optional.of(termsEnum.ord() - 1) : Optional.empty();
                        }
                        yield Optional.of(termsEnum.ord());
                    }
                    case LT -> {
                        if (seekStatus == TermsEnum.SeekStatus.END) {
                            yield Optional.of(termsEnum.ord());
                        }
                        yield ((termsEnum.ord() - 1) >= 0) ? Optional.of(termsEnum.ord() - 1) : Optional.empty();
                    }
                    default -> throw new IllegalStateException("Unexpected matchType: " + matchType);
                };
            }
        } catch (IOException e) {
            throw new RuntimeException("Error performing ordinal lookup for IP field", e);
        }
    }

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
    private BytesRef parseRawIp(String field, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        try {
            if (rawValue instanceof InetAddress) {
                return new BytesRef(InetAddressPoint.encode((InetAddress) rawValue));
            }
            if (rawValue instanceof BytesRef) {
                // Assuming it's already in the correct encoded format
                return (BytesRef) rawValue;
            }
            if (rawValue instanceof String) {
                InetAddress addr = InetAddress.getByName((String) rawValue);
                return new BytesRef(InetAddressPoint.encode(addr));
            }
            // Add handling for other raw types if necessary
            throw new IllegalArgumentException("Unsupported value type for IP field [" + field + "]: " + rawValue.getClass().getName());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse IP value for field [" + field + "]", e);
        }
    }

    @Override
    public int compareValues(Object v1, Object v2) {
        if (!(v1 instanceof BytesRef) || !(v2 instanceof BytesRef)) {
            throw new IllegalArgumentException(
                "Expected BytesRef values for IP comparison, but got " + v1.getClass().getName() + " and " + v2.getClass().getName()
            );
        }
        return ((BytesRef) v1).compareTo((BytesRef) v2);
    }
}
