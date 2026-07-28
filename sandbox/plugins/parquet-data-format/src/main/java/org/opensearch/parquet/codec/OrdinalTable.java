/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.ParquetColumnReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-segment ordinal table for {@code SortedDocValues} / {@code SortedSetDocValues} over a
 * Parquet {@code BYTE_ARRAY} (keyword/ip) column.
 *
 * <p>Built lazily on first access via the naive multi-pass column scan described in the
 * design: collect distinct terms + per-row occurrences, sort terms lex-byte ascending, assign
 * ordinals, then materialise the per-row ordinal mapping. The single-valued layout uses
 * {@code rowOrdinals} ({@code -1} = missing); the multi-valued layout uses CSR
 * ({@code csrOffsets}/{@code csrOrdinals}) with each row's slice sorted ascending so
 * {@code nextOrd()} yields ascending ordinals.
 *
 * <p>Lexicographic byte ordering matches Lucene's {@code BytesRef} natural order (unsigned
 * byte comparison), so the resulting ordinals agree with Lucene's term-ordinal contract.
 */
public final class OrdinalTable {

    /** Distinct terms in lex-byte order; {@code sortedTerms[ord]} is the term for ordinal {@code ord}. */
    private final byte[][] sortedTerms;

    /** Single-valued: ordinal per row, or {@code -1} when the row is null. */
    private final int[] rowOrdinals;

    /** Multi-valued: CSR offsets (length {@code N + 1}) into {@link #csrOrdinals}. */
    private final int[] csrOffsets;

    /** Multi-valued: flattened, per-row-ascending ordinals. */
    private final int[] csrOrdinals;

    private OrdinalTable(byte[][] sortedTerms, int[] rowOrdinals, int[] csrOffsets, int[] csrOrdinals) {
        this.sortedTerms = sortedTerms;
        this.rowOrdinals = rowOrdinals;
        this.csrOffsets = csrOffsets;
        this.csrOrdinals = csrOrdinals;
    }

    /** Number of distinct terms (ordinal count). */
    public int valueCount() {
        return sortedTerms.length;
    }

    /** Single-valued ordinal for {@code row}, or {@code -1} when missing. */
    public int ordForRow(int row) {
        return rowOrdinals[row];
    }

    /** Number of multi-valued ordinals for {@code row}. */
    public int countForRow(int row) {
        return csrOffsets[row + 1] - csrOffsets[row];
    }

    /** The {@code i}-th (0-based, ascending) multi-valued ordinal for {@code row}. */
    public int ordForRow(int row, int i) {
        return csrOrdinals[csrOffsets[row] + i];
    }

    /** Returns a fresh {@link BytesRef} over the term for {@code ord}. */
    public BytesRef lookupOrd(int ord) {
        return new BytesRef(sortedTerms[ord]);
    }

    /**
     * Builds the single-valued ordinal table by scanning {@code rows} of a keyword/ip column
     * via the reader's slow path.
     */
    public static OrdinalTable buildSingleValued(ParquetColumnReader reader, int numRows) throws IOException {
        // Pass 1: collect distinct terms and per-row occurrences.
        Map<BytesRef, List<Integer>> occurrences = new HashMap<>();
        for (int row = 0; row < numRows; row++) {
            byte[] v = reader.readBytesAtRow(row);
            if (v != null) {
                occurrences.computeIfAbsent(new BytesRef(v), k -> new ArrayList<>()).add(row);
            }
        }
        Build b = assignOrdinals(occurrences);

        // Pass 3: per-row single ordinal (-1 = missing).
        int[] rowOrdinals = new int[numRows];
        Arrays.fill(rowOrdinals, -1);
        for (Map.Entry<BytesRef, List<Integer>> e : occurrences.entrySet()) {
            int ord = b.termToOrd.get(e.getKey());
            for (int r : e.getValue()) {
                rowOrdinals[r] = ord;
            }
        }
        return new OrdinalTable(b.sortedTerms, rowOrdinals, null, null);
    }

    /**
     * Builds the multi-valued ordinal table by scanning {@code rows} of a multi-valued
     * keyword/ip column via the reader's slow path. The per-row CSR slice is sorted ascending
     * and de-duplicated (set semantics).
     */
    public static OrdinalTable buildMultiValued(ParquetColumnReader reader, int numRows) throws IOException {
        // Pass 1: collect distinct terms and, per row, the distinct set of terms present.
        Map<BytesRef, List<Integer>> occurrences = new HashMap<>();
        // Track per-row term sets so we can de-duplicate within a row (SortedSet semantics).
        List<List<BytesRef>> perRowTerms = new ArrayList<>(numRows);
        for (int row = 0; row < numRows; row++) {
            byte[][] vals = reader.readRepeatedBytesAtRow(row);
            List<BytesRef> rowTerms = new ArrayList<>();
            if (vals != null) {
                for (byte[] v : vals) {
                    BytesRef term = new BytesRef(v);
                    occurrences.computeIfAbsent(term, k -> new ArrayList<>());
                    rowTerms.add(term);
                }
            }
            perRowTerms.add(rowTerms);
        }
        Build b = assignOrdinals(occurrences);

        // Pass 3: build CSR with per-row de-duplicated, ascending ordinals.
        int[] csrOffsets = new int[numRows + 1];
        int[][] perRowOrds = new int[numRows][];
        for (int row = 0; row < numRows; row++) {
            // Distinct ordinals for this row.
            int[] ords = perRowTerms.get(row).stream().mapToInt(t -> b.termToOrd.get(t)).distinct().sorted().toArray();
            perRowOrds[row] = ords;
            csrOffsets[row + 1] = csrOffsets[row] + ords.length;
        }
        int[] csrOrdinals = new int[csrOffsets[numRows]];
        for (int row = 0; row < numRows; row++) {
            System.arraycopy(perRowOrds[row], 0, csrOrdinals, csrOffsets[row], perRowOrds[row].length);
        }
        return new OrdinalTable(b.sortedTerms, null, csrOffsets, csrOrdinals);
    }

    /** Intermediate build state: lex-sorted distinct terms + term→ordinal map. */
    private record Build(byte[][] sortedTerms, Map<BytesRef, Integer> termToOrd) {
    }

    private static Build assignOrdinals(Map<BytesRef, List<Integer>> occurrences) {
        List<BytesRef> terms = new ArrayList<>(occurrences.keySet());
        terms.sort(Comparator.naturalOrder()); // BytesRef natural order = unsigned lex-byte.
        byte[][] sortedTerms = new byte[terms.size()][];
        Map<BytesRef, Integer> termToOrd = new HashMap<>(terms.size() * 2);
        for (int ord = 0; ord < terms.size(); ord++) {
            BytesRef t = terms.get(ord);
            sortedTerms[ord] = Arrays.copyOfRange(t.bytes, t.offset, t.offset + t.length);
            termToOrd.put(t, ord);
        }
        return new Build(sortedTerms, termToOrd);
    }
}
