/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.matrix.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RunningStatsTests extends BaseMatrixStatsTestCase {

    /** test running stats */
    public void testRunningStats() throws Exception {
        final MatrixStatsResults results = new MatrixStatsResults(createRunningStats(fieldA, fieldB));
        actualStats.assertNearlyEqual(results);
    }

    /** Test merging stats across observation shards */
    public void testMergedStats() throws Exception {
        // slice observations into shards
        int numShards = randomIntBetween(2, 10);
        double obsPerShard = Math.floor(numObs / numShards);
        int start = 0;
        RunningStats stats = null;
        List<Double> fieldAShard, fieldBShard;
        for (int s = 0; s < numShards - 1; start = ++s * (int) obsPerShard) {
            fieldAShard = fieldA.subList(start, start + (int) obsPerShard);
            fieldBShard = fieldB.subList(start, start + (int) obsPerShard);
            if (stats == null) {
                stats = createRunningStats(fieldAShard, fieldBShard);
            } else {
                stats.merge(createRunningStats(fieldAShard, fieldBShard));
            }
        }
        stats.merge(createRunningStats(fieldA.subList(start, fieldA.size()), fieldB.subList(start, fieldB.size())));

        final MatrixStatsResults results = new MatrixStatsResults(stats);
        actualStats.assertNearlyEqual(results);
    }

    public void testSerializationArrays() throws IOException {
        String[] fieldNames = new String[] { "a", "b", "c" };
        RunningStats stats = new RunningStats(fieldNames);

        populateRandomStats(fieldNames, stats);
        assertFalse(stats.usesMaps());

        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RunningStats deserialized = new RunningStats(in);

        assertFalse(deserialized.usesMaps());
        assertArrayEquals(fieldNames, deserialized.fieldNames);
        assertEquals(stats.docCount, deserialized.docCount);
        assertArrayEquals(stats.countsArr, deserialized.countsArr);
        assertArrayEquals(stats.fieldSumArr, deserialized.fieldSumArr, 0d);
        assertArrayEquals(stats.meansArr, deserialized.meansArr, 0d);
        assertArrayEquals(stats.variancesArr, deserialized.variancesArr, 0d);
        assertArrayEquals(stats.skewnessArr, deserialized.skewnessArr, 0d);
        assertArrayEquals(stats.kurtosisArr, deserialized.kurtosisArr, 0d);
        assertCovarianceArraysEqual(stats.covariancesArr, deserialized.covariancesArr);
    }

    public void testSerializationMaps() throws IOException {
        String[] fieldNames = new String[] { "a", "b", "c" };
        RunningStats stats = new RunningStats(fieldNames);
        stats.switchToMaps();

        populateRandomStats(fieldNames, stats);
        assertTrue(stats.usesMaps());

        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RunningStats deserialized = new RunningStats(in);

        assertTrue(deserialized.usesMaps());
        assertArrayEquals(fieldNames, deserialized.fieldNames);
        assertEquals(stats.docCount, deserialized.docCount);
        assertEquals(stats.fieldSum, deserialized.fieldSum);
        assertEquals(stats.counts, deserialized.counts);
        assertEquals(stats.means, deserialized.means);
        assertEquals(stats.variances, deserialized.variances);
        assertEquals(stats.skewness, deserialized.skewness);
        assertEquals(stats.kurtosis, deserialized.kurtosis);
        assertEquals(stats.covariances, deserialized.covariances);
    }

    public void testImplementationsProduceSameResults() {
        int numFields = randomIntBetween(1, 5);
        String[] fieldNames = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldNames[i] = "field_" + i;
        }

        RunningStats arrayStats = new RunningStats(fieldNames);
        assertFalse(arrayStats.usesMaps());

        RunningStats mapStats = new RunningStats((String[]) null); // Ensure path w/o providing field names at all works
        assertTrue(mapStats.usesMaps());

        populateRandomStats(fieldNames, arrayStats, mapStats);

        // Check equality for each field using MatrixStatsResults wrapper
        checkEqualityWithMatrixStatsResults(fieldNames, arrayStats, mapStats);
    }

    public void testMergeDoesNotSwitchToMapsWhenBothUseArrays() {
        String[] fieldNames = new String[] { "a", "b" };
        RunningStats stats1 = new RunningStats(fieldNames);
        RunningStats stats2 = new RunningStats(fieldNames);

        populateRandomStats(fieldNames, stats1);
        populateRandomStats(fieldNames, stats2);

        assertFalse(stats1.usesMaps());
        assertFalse(stats2.usesMaps());

        stats1.merge(stats2);

        assertFalse(stats1.usesMaps());
        assertFalse(stats2.usesMaps());
        assertEquals(stats1.docCount, stats1.countsArr[0] > 0 ? stats1.docCount : stats2.docCount);

        // Assert we can't merge a RunningStats with different field names if arrays are used
        RunningStats stats3 = new RunningStats(new String[] { "a", "b", "c" });
        assertThrows(IllegalArgumentException.class, () -> stats1.merge(stats3));
    }

    public void testMergeSwitchesToMapsWhenNeeded() {
        String[] fieldNames = new String[] { "a", "b" };
        RunningStats arrayStats = new RunningStats(fieldNames);
        RunningStats statsNeedingMaps = new RunningStats(fieldNames);

        populateRandomStats(fieldNames, arrayStats);
        populateRandomStats(fieldNames, statsNeedingMaps);

        arrayStats.switchToMaps();
        assertTrue(arrayStats.usesMaps());
        assertFalse(statsNeedingMaps.usesMaps());

        long expectedDocCount = arrayStats.docCount + statsNeedingMaps.docCount;

        arrayStats.merge(statsNeedingMaps);

        assertTrue(arrayStats.usesMaps());
        assertTrue(statsNeedingMaps.usesMaps());
        assertEquals(expectedDocCount, arrayStats.docCount);
    }

    public void testSwitchBetweenImplsPreservesValues() {
        String[] fieldNames = new String[] { "a", "b", "c" };
        // We prepare multiple RunningStats here since checkEqualityWithMatrixStatsResults mutates the stored values
        RunningStats statsRoundTrip = new RunningStats(fieldNames);
        RunningStats statsOneWay = new RunningStats(fieldNames);
        RunningStats backup1 = new RunningStats(fieldNames);
        RunningStats backup2 = new RunningStats(fieldNames);

        populateRandomStats(fieldNames, statsRoundTrip, statsOneWay, backup1, backup2);
        assertFalse(statsRoundTrip.usesMaps());

        long docCountBefore = statsRoundTrip.docCount;
        long[] countsBefore = Arrays.copyOf(statsRoundTrip.countsArr, statsRoundTrip.countsArr.length);
        double[] fieldSumBefore = Arrays.copyOf(statsRoundTrip.fieldSumArr, statsRoundTrip.fieldSumArr.length);
        double[] meansBefore = Arrays.copyOf(statsRoundTrip.meansArr, statsRoundTrip.meansArr.length);
        double[] variancesBefore = Arrays.copyOf(statsRoundTrip.variancesArr, statsRoundTrip.variancesArr.length);
        double[] skewnessBefore = Arrays.copyOf(statsRoundTrip.skewnessArr, statsRoundTrip.skewnessArr.length);
        double[] kurtosisBefore = Arrays.copyOf(statsRoundTrip.kurtosisArr, statsRoundTrip.kurtosisArr.length);
        double[][] covariancesBefore = new double[statsRoundTrip.covariancesArr.length][];
        for (int i = 0; i < covariancesBefore.length; i++) {
            covariancesBefore[i] = Arrays.copyOf(statsRoundTrip.covariancesArr[i], statsRoundTrip.covariancesArr[i].length);
        }

        statsRoundTrip.switchToMaps();
        statsOneWay.switchToMaps();
        assertTrue(statsRoundTrip.usesMaps());
        assertTrue(statsOneWay.usesMaps());
        checkEqualityWithMatrixStatsResults(fieldNames, statsOneWay, backup1);

        statsRoundTrip.switchToArrays();
        assertFalse(statsRoundTrip.usesMaps());
        assertEquals(docCountBefore, statsRoundTrip.docCount);
        assertArrayEquals(countsBefore, statsRoundTrip.countsArr);
        assertArrayEquals(fieldSumBefore, statsRoundTrip.fieldSumArr, 0d);
        assertArrayEquals(meansBefore, statsRoundTrip.meansArr, 0d);
        assertArrayEquals(variancesBefore, statsRoundTrip.variancesArr, 0d);
        assertArrayEquals(skewnessBefore, statsRoundTrip.skewnessArr, 0d);
        assertArrayEquals(kurtosisBefore, statsRoundTrip.kurtosisArr, 0d);
        assertCovarianceArraysEqual(covariancesBefore, statsRoundTrip.covariancesArr);
        checkEqualityWithMatrixStatsResults(fieldNames, statsRoundTrip, backup2);
    }

    private void assertCovarianceArraysEqual(double[][] expected, double[][] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(expected[i], actual[i], 0d);
        }
    }

    private void checkEqualityWithMatrixStatsResults(String[] fieldNames, RunningStats stats1, RunningStats stats2) {
        // Check equality for each field using MatrixStatsResults wrapper, which is how final result of agg will do it
        MatrixStatsResults results1 = new MatrixStatsResults(stats1);
        MatrixStatsResults results2 = new MatrixStatsResults(stats2);

        assertEquals(results1.getDocCount(), results2.getDocCount());
        final double delta = 1e-9;
        for (String field : fieldNames) {
            assertEquals(results1.getFieldCount(field), results2.getFieldCount(field));
            assertEquals(results1.getMean(field), results2.getMean(field), delta);
            assertEquals(results1.getVariance(field), results2.getVariance(field), delta);
            assertEquals(results1.getSkewness(field), results2.getSkewness(field), delta);
            assertEquals(results1.getKurtosis(field), results2.getKurtosis(field), delta);
        }
        for (String fieldX : fieldNames) {
            for (String fieldY : fieldNames) {
                assertEquals(results1.getCovariance(fieldX, fieldY), results2.getCovariance(fieldX, fieldY), delta);
                double corr1 = results1.getCorrelation(fieldX, fieldY);
                double corr2 = results2.getCorrelation(fieldX, fieldY);
                if (Double.isNaN(corr2) || Double.isNaN(corr1)) {
                    assertTrue(Double.isNaN(corr2) && Double.isNaN(corr1));
                } else {
                    assertEquals(corr1, corr2, delta);
                }
            }
        }
    }

    /**
     * Add the same random field values to each RunningStats
     */
    private void populateRandomStats(String[] fieldNames, RunningStats... statsList) {
        double[] values = new double[fieldNames.length];
        int numDocs = randomIntBetween(20, 100);
        for (int i = 0; i < numDocs; i++) {
            for (int f = 0; f < fieldNames.length; f++) {
                values[f] = randomDouble();
            }
            for (RunningStats stats : statsList) {
                stats.add(fieldNames, values);
            }
        }
    }

    private RunningStats createRunningStats(List<Double> fieldAObs, List<Double> fieldBObs) {
        // create a document with two numeric fields
        final String[] fieldNames = new String[2];
        fieldNames[0] = fieldAKey;
        fieldNames[1] = fieldBKey;
        RunningStats stats = new RunningStats(fieldNames);
        final double[] fieldVals = new double[2];

        // running stats computation
        for (int n = 0; n < fieldAObs.size(); ++n) {
            fieldVals[0] = fieldAObs.get(n);
            fieldVals[1] = fieldBObs.get(n);
            stats.add(fieldNames, fieldVals);
        }
        return stats;
    }

}
