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

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final correlation and covariance stats
 * based on these descriptive stats. This single pass, parallel approach is based on:
 * <p>
 * http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
 *
 * Map-based implementation is left here for backwards compatibility purposes. It's much faster to use
 * primitive arrays, as the keys of the map are constant (they're just the fields of the aggregation).
 * However, when deserializing RunningStats from previous versions, we don't have the sorted list of field names,
 * so we must revert to using maps. The same goes for any RunningStats that merges with these deserialized instances.
 * This should only happen in mixed clusters.
 */
public class RunningStats implements Writeable, Cloneable {
    /** Whether it uses the slow maps impl or not. If this is true, the Maps containing data are guaranteed to be non-null,
     * and if it's false the arrays are guaranteed to be non-null after ctor returns.
     */
    private boolean usesMaps = false;

    public static final Version ARRAY_IMPL_VERSION = Version.V_3_4_0;
    /** count of observations (same number of observations per field) */
    protected long docCount = 0;

    // Array-based values. These may be null for RunningStats from older versions
    // or ones which have added one from an older version.
    protected double[] fieldSumArr;
    protected long[] countsArr;
    protected double[] meansArr;
    protected double[] variancesArr;
    protected double[] skewnessArr;
    protected double[] kurtosisArr;
    protected double[][] covariancesArr;

    final String[] fieldNames;

    // Old map-based values
    /** per field sum of observations */
    protected Map<String, Double> fieldSum;
    /** counts */
    protected Map<String, Long> counts;
    /** mean values (first moment) */
    protected Map<String, Double> means;
    /** variance values (second moment) */
    protected Map<String, Double> variances;
    /** skewness values (third moment) */
    protected Map<String, Double> skewness;
    /** kurtosis values (fourth moment) */
    protected Map<String, Double> kurtosis;
    /** covariance values */
    protected Map<String, Map<String, Double>> covariances;

    RunningStats(final String[] fieldNames) {
        this.fieldNames = fieldNames;
        init(fieldNames == null);
    }

    RunningStats(final String[] fieldNames, final double[] fieldVals) {
        this.fieldNames = fieldNames;
        if (fieldVals != null && fieldVals.length > 0) {
            init(fieldNames == null);
            this.add(fieldNames, fieldVals);
        }
    }

    private void init(boolean usesMaps) {
        this.usesMaps = usesMaps;
        if (usesMaps) {
            counts = new HashMap<>();
            fieldSum = new HashMap<>();
            means = new HashMap<>();
            skewness = new HashMap<>();
            kurtosis = new HashMap<>();
            covariances = new HashMap<>();
            variances = new HashMap<>();
        } else {
            assert fieldNames != null;
            countsArr = new long[fieldNames.length];
            fieldSumArr = new double[fieldNames.length];
            meansArr = new double[fieldNames.length];
            skewnessArr = new double[fieldNames.length];
            kurtosisArr = new double[fieldNames.length];
            variancesArr = new double[fieldNames.length];
            covariancesArr = new double[fieldNames.length][fieldNames.length];
        }
    }

    void switchToMaps() {
        // Switch from the fast primitive array implementation to the slower maps implementation.
        if (usesMaps) return;
        counts = convertLongArrayToMap(countsArr);
        fieldSum = convertDoubleArrayToMap(fieldSumArr);
        means = convertDoubleArrayToMap(meansArr);
        skewness = convertDoubleArrayToMap(skewnessArr);
        kurtosis = convertDoubleArrayToMap(kurtosisArr);
        variances = convertDoubleArrayToMap(variancesArr);
        covariances = convertNestedDoubleArrayToMap(covariancesArr);
        usesMaps = true;
    }

    void switchToArrays() {
        // Switch from the slower maps implementation to the fast primitive array implementation.
        if (fieldNames == null) throw new IllegalArgumentException("Cannot convert to array impl if fieldNames is null");
        countsArr = convertFieldLongMap(counts);
        fieldSumArr = convertFieldDoubleMap(fieldSum);
        meansArr = convertFieldDoubleMap(means);
        skewnessArr = convertFieldDoubleMap(skewness);
        kurtosisArr = convertFieldDoubleMap(kurtosis);
        variancesArr = convertFieldDoubleMap(variances);
        covariancesArr = convertNestedDoubleMap(covariances);
        usesMaps = false;
    }

    /** Ctor to create an instance of running statistics */
    @SuppressWarnings("unchecked")
    public RunningStats(StreamInput in) throws IOException {
        docCount = (Long) in.readGenericValue();
        fieldSum = (Map<String, Double>) in.readGenericValue();
        counts = (Map<String, Long>) in.readGenericValue();
        means = (Map<String, Double>) in.readGenericValue();
        variances = (Map<String, Double>) in.readGenericValue();
        skewness = (Map<String, Double>) in.readGenericValue();
        kurtosis = (Map<String, Double>) in.readGenericValue();
        covariances = (Map<String, Map<String, Double>>) in.readGenericValue();

        if (in.getVersion().onOrAfter(ARRAY_IMPL_VERSION)) {
            this.usesMaps = in.readBoolean();
            this.fieldNames = in.readOptionalStringArray();
            if (!usesMaps) {
                switchToArrays();
            }
        } else {
            this.usesMaps = true;
            this.fieldNames = null;
        }
    }

    double[] convertFieldDoubleMap(Map<String, Double> serializedMap) {
        assert serializedMap.keySet().equals(new HashSet<>(Arrays.asList(fieldNames)));
        double[] result = new double[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            result[i] = serializedMap.get(fieldNames[i]);
        }
        return result;
    }

    long[] convertFieldLongMap(Map<String, Long> serializedMap) {
        assert serializedMap.keySet().equals(new HashSet<>(Arrays.asList(fieldNames)));
        long[] result = new long[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            result[i] = serializedMap.get(fieldNames[i]);
        }
        return result;
    }

    double[][] convertNestedDoubleMap(Map<String, Map<String, Double>> serializedMap) {
        assert serializedMap.keySet().equals(new HashSet<>(Arrays.asList(fieldNames)));
        double[][] result = new double[fieldNames.length][fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            for (int j = i + 1; j < fieldNames.length; j++) {
                result[i][j] = serializedMap.get(fieldNames[i]).get(fieldNames[j]);
            }
        }
        return result;
    }

    Map<String, Double> convertDoubleArrayToMap(double[] stats) {
        Map<String, Double> result = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            result.put(fieldNames[i], stats[i]);
        }
        return result;
    }

    Map<String, Long> convertLongArrayToMap(long[] stats) {
        Map<String, Long> result = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            result.put(fieldNames[i], stats[i]);
        }
        return result;
    }

    Map<String, Map<String, Double>> convertNestedDoubleArrayToMap(double[][] stats) {
        Map<String, Map<String, Double>> result = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            Map<String, Double> innerMap = new HashMap<>();
            result.put(fieldNames[i], innerMap);
            for (int j = i + 1; j < fieldNames.length; j++) {
                innerMap.put(fieldNames[j], stats[i][j]);
            }
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Always write as maps, which is how earlier versions did it. Receiving node will convert to arrays if needed.
        out.writeGenericValue(docCount);
        if (usesMaps) {
            // If we are already using maps we can send them as-is.
            out.writeGenericValue(fieldSum);
            out.writeGenericValue(counts);
            out.writeGenericValue(means);
            out.writeGenericValue(variances);
            out.writeGenericValue(skewness);
            out.writeGenericValue(kurtosis);
            out.writeGenericValue(covariances);
        } else {
            // If we aren't using maps already we must convert to send. This will still be backwards compatible.
            out.writeGenericValue(convertDoubleArrayToMap(fieldSumArr));
            out.writeGenericValue(convertLongArrayToMap(countsArr));
            out.writeGenericValue(convertDoubleArrayToMap(meansArr));
            out.writeGenericValue(convertDoubleArrayToMap(variancesArr));
            out.writeGenericValue(convertDoubleArrayToMap(skewnessArr));
            out.writeGenericValue(convertDoubleArrayToMap(kurtosisArr));
            out.writeGenericValue(convertNestedDoubleArrayToMap(covariancesArr));
        }

        if (out.getVersion().onOrAfter(ARRAY_IMPL_VERSION)) {
            out.writeBoolean(usesMaps);
            out.writeOptionalStringArray(fieldNames);
        }
    }

    /** updates running statistics with a documents field values **/
    public void add(final String[] fn, final double[] fieldVals) {
        if (fieldVals == null) {
            throw new IllegalArgumentException("Cannot add statistics without field values.");
        }
        if (usesMaps) {
            addMaps(fn, fieldVals);
        } else {
            addArrays(fieldVals);
        }
    }

    public void addArrays(final double[] fieldVals) {
        if (fieldNames.length != fieldVals.length) {
            throw new IllegalArgumentException("Number of field values do not match number of field names.");
        }

        // update total, mean, and variance
        ++docCount;
        double fieldValue;
        double m2, m3; // moments
        double d, dn, dn2, t1;
        final double[] deltas = new double[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldValue = fieldVals[i];
            // update counts
            countsArr[i]++;
            // update running sum
            fieldSumArr[i] += fieldValue;
            // update running deltas
            deltas[i] = fieldValue * docCount - fieldSumArr[i];

            // update running mean, variance, skewness, kurtosis
            if (docCount == 1) {
                meansArr[i] = fieldValue;
                // variances, skewness, kurtosis already initialized to 0
            } else {
                // update running means
                d = fieldValue - meansArr[i];
                dn = d / docCount;
                meansArr[i] += dn;
                // update running variances
                m2 = variancesArr[i];
                t1 = d * dn * (docCount - 1);
                variancesArr[i] += t1;
                // update running skewnesses
                m3 = skewnessArr[i];
                skewnessArr[i] += (t1 * dn * (docCount - 2D) - 3D * dn * m2);
                dn2 = dn * dn;
                kurtosisArr[i] += t1 * dn2 * (docCount * docCount - 3D * docCount + 3D) + 6D * dn2 * m2 - 4D * dn * m3;
            }
        }
        this.updateCovarianceArrays(fieldNames, deltas);
    }

    public void addMaps(final String[] fieldNames, final double[] fieldVals) {
        if (fieldNames == null) {
            throw new IllegalArgumentException("Cannot add statistics without field names.");
        } else if (fieldNames.length != fieldVals.length) {
            throw new IllegalArgumentException("Number of field values do not match number of field names.");
        }

        // update total, mean, and variance
        ++docCount;
        String fieldName;
        double fieldValue;
        double m1, m2, m3, m4;  // moments
        double d, dn, dn2, t1;
        final HashMap<String, Double> deltas = new HashMap<>();
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldName = fieldNames[i];
            fieldValue = fieldVals[i];

            // update counts
            counts.put(fieldName, 1 + (counts.containsKey(fieldName) ? counts.get(fieldName) : 0));
            // update running sum
            fieldSum.put(fieldName, fieldValue + (fieldSum.containsKey(fieldName) ? fieldSum.get(fieldName) : 0));
            // update running deltas
            deltas.put(fieldName, fieldValue * docCount - fieldSum.get(fieldName));

            // update running mean, variance, skewness, kurtosis
            if (means.containsKey(fieldName)) {
                // update running means
                m1 = means.get(fieldName);
                d = fieldValue - m1;
                means.put(fieldName, m1 + d / docCount);
                // update running variances
                dn = d / docCount;
                t1 = d * dn * (docCount - 1);
                m2 = variances.get(fieldName);
                variances.put(fieldName, m2 + t1);
                m3 = skewness.get(fieldName);
                skewness.put(fieldName, m3 + (t1 * dn * (docCount - 2D) - 3D * dn * m2));
                dn2 = dn * dn;
                m4 = t1 * dn2 * (docCount * docCount - 3D * docCount + 3D) + 6D * dn2 * m2 - 4D * dn * m3;
                kurtosis.put(fieldName, kurtosis.get(fieldName) + m4);
            } else {
                means.put(fieldName, fieldValue);
                variances.put(fieldName, 0.0);
                skewness.put(fieldName, 0.0);
                kurtosis.put(fieldName, 0.0);
            }
        }

        this.updateCovarianceMaps(fieldNames, deltas);
    }

    private void updateCovarianceArrays(final String[] fieldNames, final double[] deltas) {
        if (docCount > 1) {
            for (int i = 0; i < fieldNames.length; ++i) {
                for (int j = i + 1; j < fieldNames.length; j++) {
                    double intermediate = 1.0 / (docCount * (docCount - 1.0)) * deltas[i] * deltas[j];
                    covariancesArr[i][j] += intermediate;
                }
            }
        }
    }

    /** Update covariance matrix */
    private void updateCovarianceMaps(final String[] fieldNames, final Map<String, Double> deltas) {
        // deep copy of hash keys (field names)
        ArrayList<String> cFieldNames = new ArrayList<>(Arrays.asList(fieldNames));
        String fieldName;
        double dR, newVal;
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldName = fieldNames[i];
            cFieldNames.remove(fieldName);
            // update running covariances
            dR = deltas.get(fieldName);
            Map<String, Double> cFieldVals = (covariances.get(fieldName) != null) ? covariances.get(fieldName) : new HashMap<>();
            for (String cFieldName : cFieldNames) {
                if (cFieldVals.containsKey(cFieldName)) {
                    newVal = cFieldVals.get(cFieldName) + 1.0 / (docCount * (docCount - 1.0)) * dR * deltas.get(cFieldName);
                    cFieldVals.put(cFieldName, newVal);
                } else {
                    cFieldVals.put(cFieldName, 0.0);
                }
            }
            if (cFieldVals.size() > 0) {
                covariances.put(fieldName, cFieldVals);
            }
        }
    }

    /**
     * Merges the descriptive statistics of a second data set (e.g., per shard)
     * <p>
     * running computations taken from: http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf
     **/
    public void merge(final RunningStats other) {
        if (other == null) {
            return;
        }
        if (!usesMaps && !other.usesMaps) {
            mergeArrays(other);
            return;
        }
        // If at least one of the two RunningStats uses the maps impl, ensure both switch to it before merge
        if (!usesMaps) {
            switchToMaps();
        } else if (!other.usesMaps) {
            other.switchToMaps();
        }
        mergeMaps(other);
    }

    private void mergeArrays(final RunningStats other) {
        assert !other.usesMaps && !this.usesMaps;

        if (!Arrays.equals(other.fieldNames, fieldNames)) {
            throw new IllegalArgumentException("Cannot merge RunningStats with different fieldNames");
        } else if (this.docCount == 0) {
            this.meansArr = other.meansArr;
            this.countsArr = other.countsArr;
            this.fieldSumArr = other.fieldSumArr;
            this.variancesArr = other.variancesArr;
            this.skewnessArr = other.skewnessArr;
            this.kurtosisArr = other.kurtosisArr;
            this.covariancesArr = other.covariancesArr;
            this.docCount = other.docCount;
            return;
        }
        final double nA = docCount;
        final double nB = other.docCount;
        // merge count
        docCount += other.docCount;

        final double[] deltas = new double[fieldNames.length];
        double meanA, varA, skewA, kurtA, meanB, varB, skewB, kurtB;
        double d, d2, d3, d4, n2, nA2, nB2;
        double newSkew, nk;
        // across fields
        for (int i = 0; i < fieldNames.length; i++) {
            meanA = meansArr[i];
            varA = variancesArr[i];
            skewA = skewnessArr[i];
            kurtA = kurtosisArr[i];
            meanB = other.meansArr[i];
            varB = other.variancesArr[i];
            skewB = other.skewnessArr[i];
            kurtB = other.kurtosisArr[i];

            // merge counts of two sets
            countsArr[i] += other.countsArr[i];

            // merge means of two sets
            meansArr[i] = (nA * meansArr[i] + nB * other.meansArr[i]) / (nA + nB);

            // merge deltas
            deltas[i] = other.fieldSumArr[i] / nB - fieldSumArr[i] / nA;

            // merge totals
            fieldSumArr[i] += other.fieldSumArr[i];

            // merge variances, skewness, and kurtosis of two sets
            d = meanB - meanA;          // delta mean
            d2 = d * d;                 // delta mean squared
            d3 = d * d2;                // delta mean cubed
            d4 = d2 * d2;               // delta mean 4th power
            n2 = docCount * docCount;   // num samples squared
            nA2 = nA * nA;              // doc A num samples squared
            nB2 = nB * nB;              // doc B num samples squared
            // variance
            variancesArr[i] = varA + varB + d2 * nA * other.docCount / docCount;
            // skewness
            newSkew = skewA + skewB + d3 * nA * nB * (nA - nB) / n2;
            skewnessArr[i] = newSkew + 3D * d * (nA * varB - nB * varA) / docCount;
            // kurtosis
            nk = kurtA + kurtB + d4 * nA * nB * (nA2 - nA * nB + nB2) / (n2 * docCount);
            kurtosisArr[i] = nk + 6D * d2 * (nA2 * varB + nB2 * varA) / n2 + 4D * d * (nA * skewB - nB * skewA) / docCount;
        }

        this.mergeCovarianceArrays(other, deltas);
    }

    private void mergeCovarianceArrays(final RunningStats other, final double[] deltas) {
        double f = ((double) (docCount - other.docCount)) * other.docCount / this.docCount; // , dR, newVal;
        for (int i = 0; i < fieldNames.length; i++) {
            for (int j = i + 1; j < fieldNames.length; j++) {
                covariancesArr[i][j] += other.covariancesArr[i][j] + f * deltas[i] * deltas[j];
            }
        }
    }

    private void mergeMaps(final RunningStats other) {
        assert other.usesMaps && this.usesMaps;
        if (this.docCount == 0) {
            for (Map.Entry<String, Double> fs : other.means.entrySet()) {
                final String fieldName = fs.getKey();
                this.means.put(fieldName, fs.getValue().doubleValue());
                this.counts.put(fieldName, other.counts.get(fieldName).longValue());
                this.fieldSum.put(fieldName, other.fieldSum.get(fieldName).doubleValue());
                this.variances.put(fieldName, other.variances.get(fieldName).doubleValue());
                this.skewness.put(fieldName, other.skewness.get(fieldName).doubleValue());
                this.kurtosis.put(fieldName, other.kurtosis.get(fieldName).doubleValue());
                if (other.covariances.containsKey(fieldName)) {
                    this.covariances.put(fieldName, other.covariances.get(fieldName));
                }
                this.docCount = other.docCount;
            }
            return;
        }
        final double nA = docCount;
        final double nB = other.docCount;
        // merge count
        docCount += other.docCount;

        final HashMap<String, Double> deltas = new HashMap<>();
        double meanA, varA, skewA, kurtA, meanB, varB, skewB, kurtB;
        double d, d2, d3, d4, n2, nA2, nB2;
        double newSkew, nk;
        // across fields
        for (Map.Entry<String, Double> fs : other.means.entrySet()) {
            final String fieldName = fs.getKey();
            meanA = means.get(fieldName);
            varA = variances.get(fieldName);
            skewA = skewness.get(fieldName);
            kurtA = kurtosis.get(fieldName);
            meanB = other.means.get(fieldName);
            varB = other.variances.get(fieldName);
            skewB = other.skewness.get(fieldName);
            kurtB = other.kurtosis.get(fieldName);

            // merge counts of two sets
            counts.put(fieldName, counts.get(fieldName) + other.counts.get(fieldName));

            // merge means of two sets
            means.put(fieldName, (nA * means.get(fieldName) + nB * other.means.get(fieldName)) / (nA + nB));

            // merge deltas
            deltas.put(fieldName, other.fieldSum.get(fieldName) / nB - fieldSum.get(fieldName) / nA);

            // merge totals
            fieldSum.put(fieldName, fieldSum.get(fieldName) + other.fieldSum.get(fieldName));

            // merge variances, skewness, and kurtosis of two sets
            d = meanB - meanA;          // delta mean
            d2 = d * d;                 // delta mean squared
            d3 = d * d2;                // delta mean cubed
            d4 = d2 * d2;               // delta mean 4th power
            n2 = docCount * docCount;   // num samples squared
            nA2 = nA * nA;              // doc A num samples squared
            nB2 = nB * nB;              // doc B num samples squared
            // variance
            variances.put(fieldName, varA + varB + d2 * nA * other.docCount / docCount);
            // skeewness
            newSkew = skewA + skewB + d3 * nA * nB * (nA - nB) / n2;
            skewness.put(fieldName, newSkew + 3D * d * (nA * varB - nB * varA) / docCount);
            // kurtosis
            nk = kurtA + kurtB + d4 * nA * nB * (nA2 - nA * nB + nB2) / (n2 * docCount);
            kurtosis.put(fieldName, nk + 6D * d2 * (nA2 * varB + nB2 * varA) / n2 + 4D * d * (nA * skewB - nB * skewA) / docCount);
        }

        this.mergeCovarianceMaps(other, deltas);
    }

    /** Merges two covariance matrices */
    private void mergeCovarianceMaps(final RunningStats other, final Map<String, Double> deltas) {
        final double countA = docCount - other.docCount;
        double f, dR, newVal;
        for (Map.Entry<String, Double> fs : other.means.entrySet()) {
            final String fieldName = fs.getKey();
            // merge covariances of two sets
            f = countA * other.docCount / this.docCount;
            dR = deltas.get(fieldName);
            // merge covariances
            if (covariances.containsKey(fieldName)) {
                Map<String, Double> cFieldVals = covariances.get(fieldName);
                for (String cFieldName : cFieldVals.keySet()) {
                    newVal = cFieldVals.get(cFieldName);
                    if (other.covariances.containsKey(fieldName) && other.covariances.get(fieldName).containsKey(cFieldName)) {
                        newVal += other.covariances.get(fieldName).get(cFieldName) + f * dR * deltas.get(cFieldName);
                    } else {
                        newVal += other.covariances.get(cFieldName).get(fieldName) + f * dR * deltas.get(cFieldName);
                    }
                    cFieldVals.put(cFieldName, newVal);
                }
                covariances.put(fieldName, cFieldVals);
            }
        }
    }

    @Override
    public RunningStats clone() {
        try {
            return (RunningStats) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new OpenSearchException("Error trying to create a copy of RunningStats");
        }
    }

    public boolean usesMaps() {
        return usesMaps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RunningStats that = (RunningStats) o;
        if (usesMaps != that.usesMaps) return false;
        if (usesMaps) {
            return docCount == that.docCount
                && Objects.equals(fieldSum, that.fieldSum)
                && Objects.equals(counts, that.counts)
                && Objects.equals(means, that.means)
                && Objects.equals(variances, that.variances)
                && Objects.equals(skewness, that.skewness)
                && Objects.equals(kurtosis, that.kurtosis)
                && Objects.equals(covariances, that.covariances);
        }
        return docCount == that.docCount
            && Arrays.equals(fieldSumArr, that.fieldSumArr)
            && Arrays.equals(countsArr, that.countsArr)
            && Arrays.equals(meansArr, that.meansArr)
            && Arrays.equals(variancesArr, that.variancesArr)
            && Arrays.equals(skewnessArr, that.skewnessArr)
            && Arrays.equals(kurtosisArr, that.kurtosisArr)
            && Arrays.deepEquals(covariancesArr, that.covariancesArr);
    }

    @Override
    public int hashCode() {
        if (usesMaps) {
            return Objects.hash(docCount, fieldSum, counts, means, variances, skewness, kurtosis, covariances);
        }
        return Objects.hash(
            docCount,
            Arrays.hashCode(fieldSumArr),
            Arrays.hashCode(countsArr),
            Arrays.hashCode(meansArr),
            Arrays.hashCode(variancesArr),
            Arrays.hashCode(skewnessArr),
            Arrays.hashCode(kurtosisArr),
            Arrays.deepHashCode(covariancesArr)
        );
    }
}
