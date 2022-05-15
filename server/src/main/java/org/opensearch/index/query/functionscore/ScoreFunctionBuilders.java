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

package org.opensearch.index.query.functionscore;

import org.opensearch.common.Nullable;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import static java.util.Collections.emptyMap;

/**
 * Static method aliases for constructors of known {@link ScoreFunctionBuilder}s.
 *
 * @opensearch.internal
 */
public class ScoreFunctionBuilders {

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object origin, Object scale) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, null);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        @Nullable String functionName
    ) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, null, functionName);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object origin, Object scale, Object offset) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, offset);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        @Nullable String functionName
    ) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, offset, functionName);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay
    ) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay,
        @Nullable String functionName
    ) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, offset, decay, functionName);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, null);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        @Nullable String functionName
    ) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, null, functionName);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale, Object offset) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, offset);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale, Object offset, double decay) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay,
        @Nullable String functionName
    ) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, offset, decay, functionName);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        @Nullable String functionName
    ) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, null, functionName);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object origin, Object scale) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, null);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object origin, Object scale, Object offset) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        @Nullable String functionName
    ) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset, functionName);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay
    ) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay,
        @Nullable String functionName
    ) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset, decay, functionName);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(Script script) {
        return scriptFunction(script, null);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script) {
        return scriptFunction(script, null);
    }

    public static RandomScoreFunctionBuilder randomFunction() {
        return randomFunction(null);
    }

    public static WeightBuilder weightFactorFunction(float weight) {
        return weightFactorFunction(weight, null);
    }

    public static FieldValueFactorFunctionBuilder fieldValueFactorFunction(String fieldName) {
        return fieldValueFactorFunction(fieldName, null);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(Script script, @Nullable String functionName) {
        return new ScriptScoreFunctionBuilder(script, functionName);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script, @Nullable String functionName) {
        return new ScriptScoreFunctionBuilder(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, script, emptyMap()), functionName);
    }

    public static RandomScoreFunctionBuilder randomFunction(@Nullable String functionName) {
        return new RandomScoreFunctionBuilder(functionName);
    }

    public static WeightBuilder weightFactorFunction(float weight, @Nullable String functionName) {
        return (WeightBuilder) (new WeightBuilder(functionName).setWeight(weight));
    }

    public static FieldValueFactorFunctionBuilder fieldValueFactorFunction(String fieldName, @Nullable String functionName) {
        return new FieldValueFactorFunctionBuilder(fieldName, functionName);
    }
}
