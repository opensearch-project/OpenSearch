/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TermBasedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;

/**
 * Collects the smallest terms of a field in one reader. Each segment's terms come from
 * {@link TermBasedFieldType#termsEnum}, and the first {@code size} distinct terms across all segments are kept.
 * Like any term dictionary lookup, this can return terms of deleted documents until a merge removes them, and terms
 * of documents that document-level security hides.
 *
 * @opensearch.internal
 */
public final class TermsEnumCollector {

    /** How many terms are visited between two deadline checks. */
    static final int CHECK_INTERVAL = 128;

    /**
     * The terms collected from one reader.
     *
     * @param terms the display strings of the terms, in ascending byte order
     * @param timedOut whether the deadline stopped the collection before it was complete
     */
    public record Result(List<String> terms, boolean timedOut) {
        /** Creates a result, copying {@code terms}. */
        public Result {
            terms = List.copyOf(terms);
        }
    }

    private TermsEnumCollector() {}

    /**
     * Collects up to {@code size} terms that start with {@code prefix} and sort after {@code searchAfter}.
     *
     * @param fieldType the field whose terms are collected
     * @param reader the reader whose segments are read
     * @param prefix the prefix every term starts with
     * @param caseInsensitive whether to match the prefix with ASCII case folding
     * @param searchAfter only terms strictly greater than this are collected, or {@code null} for no lower bound
     * @param size the maximum number of terms to collect, at least 1
     * @param timedOut polled periodically; once it returns {@code true}, the terms collected so far are returned.
     *                 Any exception it throws, such as a {@code TaskCancelledException}, propagates.
     * @return the collected terms and whether the deadline cut the collection short
     * @throws IOException if reading the index fails
     */
    public static Result collect(
        TermBasedFieldType fieldType,
        IndexReader reader,
        String prefix,
        boolean caseInsensitive,
        @Nullable BytesRef searchAfter,
        int size,
        BooleanSupplier timedOut
    ) throws IOException {
        if (size < 1) {
            throw new IllegalArgumentException("size must be at least 1 but was [" + size + "]");
        }
        TreeSet<BytesRef> smallest = new TreeSet<>();
        long visited = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            TermsEnum termsEnum = fieldType.termsEnum(leaf.reader(), prefix, caseInsensitive, searchAfter);
            if (termsEnum == null) {
                continue;
            }
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                if (smallest.size() == size && term.compareTo(smallest.last()) >= 0) {
                    break; // the rest of this segment sorts after every term kept
                }
                if (searchAfter == null || term.compareTo(searchAfter) > 0) {
                    smallest.add(BytesRef.deepCopyOf(term));
                    if (smallest.size() > size) {
                        smallest.pollLast();
                    }
                }
                if (++visited % CHECK_INTERVAL == 0 && timedOut.getAsBoolean()) {
                    return result(fieldType, smallest, true);
                }
            }
        }
        return result(fieldType, smallest, false);
    }

    private static Result result(MappedFieldType fieldType, TreeSet<BytesRef> terms, boolean timedOut) {
        List<String> display = new ArrayList<>(terms.size());
        for (BytesRef term : terms) {
            display.add(displayString(fieldType, term));
        }
        return new Result(display, timedOut);
    }

    /**
     * Renders a term the way the field type displays its values.
     *
     * @param fieldType the field type the term belongs to
     * @param term the indexed term
     * @return {@code fieldType.valueForDisplay(term)} as a string; a {@link BytesRef} is decoded as UTF-8
     */
    public static String displayString(MappedFieldType fieldType, BytesRef term) {
        Object value = fieldType.valueForDisplay(term);
        return value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString();
    }
}
