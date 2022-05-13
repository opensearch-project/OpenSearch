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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptFactory;

import java.util.Map;

/**
 * This class provides a custom script context for the Moving Function pipeline aggregation,
 * so that we can expose a number of pre-baked moving functions like min, max, movavg, etc
 *
 * @opensearch.internal
 */
public abstract class MovingFunctionScript {
    /**
     * @param params The user-provided parameters
     * @param values The values in the window that we are moving a function across
     * @return A double representing the value from this particular window
     */
    public abstract double execute(Map<String, Object> params, double[] values);

    /**
     * Factory interface
     *
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        MovingFunctionScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] { "params", "values" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("moving-function", Factory.class);
}
