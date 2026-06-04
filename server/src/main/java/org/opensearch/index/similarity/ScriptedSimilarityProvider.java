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

package org.opensearch.index.similarity;

import org.apache.lucene.search.similarities.Similarity;
import org.opensearch.Version;
import org.opensearch.common.TriFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.script.SimilarityScript;
import org.opensearch.script.SimilarityWeightScript;

/**
 * Provider of scripted similarities.
 *
 * @opensearch.internal
 */
final class ScriptedSimilarityProvider implements TriFunction<Settings, Version, ScriptService, Similarity> {

    @Override
    public Similarity apply(Settings settings, Version indexCreatedVersion, ScriptService scriptService) {
        boolean discountOverlaps = settings.getAsBoolean(SimilarityProviders.DISCOUNT_OVERLAPS, true);
        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        SimilarityScript.Factory scriptFactory = scriptService.compile(script, SimilarityScript.CONTEXT);
        Settings weightScriptSettings = settings.getAsSettings("weight_script");
        Script weightScript = null;
        SimilarityWeightScript.Factory weightScriptFactory = null;
        if (weightScriptSettings.isEmpty() == false) {
            weightScript = Script.parse(weightScriptSettings);
            weightScriptFactory = scriptService.compile(weightScript, SimilarityWeightScript.CONTEXT);
        }
        return new ScriptedSimilarity(
            weightScript == null ? null : weightScript.toString(),
            weightScriptFactory == null ? null : weightScriptFactory::newInstance,
            script.toString(),
            scriptFactory::newInstance,
            discountOverlaps
        );
    }

}
