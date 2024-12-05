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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.Booleans;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the logic of whether a new index should be automatically created when
 * a write operation is about to happen in a non existing index.
 *
 * @opensearch.internal
 */
public final class AutoCreateIndex {

    public static final Setting<AutoCreate> AUTO_CREATE_INDEX_SETTING = new Setting<>(
        "action.auto_create_index",
        "true",
        AutoCreate::new,
        Property.NodeScope,
        Setting.Property.Dynamic
    );
    private final IndexNameExpressionResolver resolver;
    private final SystemIndices systemIndices;
    private volatile AutoCreate autoCreate;

    public AutoCreateIndex(
        Settings settings,
        ClusterSettings clusterSettings,
        IndexNameExpressionResolver resolver,
        SystemIndices systemIndices
    ) {
        this.resolver = resolver;
        this.systemIndices = systemIndices;
        this.autoCreate = AUTO_CREATE_INDEX_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AUTO_CREATE_INDEX_SETTING, this::setAutoCreate);
    }

    /**
     * Do we really need to check if an index should be auto created?
     */
    public boolean needToCheck() {
        return this.autoCreate.autoCreateIndex;
    }

    /**
     * Should the index be auto created?
     * @throws IndexNotFoundException if the index doesn't exist and shouldn't be auto created
     */
    public boolean shouldAutoCreate(String index, ClusterState state) {
        if (resolver.hasIndexAbstraction(index, state)) {
            return false;
        }

        // Always auto-create system indexes
        if (systemIndices.isSystemIndex(index)) {
            return true;
        }

        // One volatile read, so that all checks are done against the same instance:
        final AutoCreate autoCreate = this.autoCreate;
        if (autoCreate.autoCreateIndex == false) {
            throw new IndexNotFoundException("[" + AUTO_CREATE_INDEX_SETTING.getKey() + "] is [false]", index);
        }
        // matches not set, default value of "true"
        if (autoCreate.expressions.isEmpty()) {
            return true;
        }
        for (Tuple<String, Boolean> expression : autoCreate.expressions) {
            String indexExpression = expression.v1();
            boolean include = expression.v2();
            if (Regex.simpleMatch(indexExpression, index)) {
                if (include) {
                    return true;
                }
                throw new IndexNotFoundException(
                    "["
                        + AUTO_CREATE_INDEX_SETTING.getKey()
                        + "] contains [-"
                        + indexExpression
                        + "] which forbids automatic creation of the index",
                    index
                );
            }
        }
        throw new IndexNotFoundException("[" + AUTO_CREATE_INDEX_SETTING.getKey() + "] ([" + autoCreate + "]) doesn't match", index);
    }

    AutoCreate getAutoCreate() {
        return autoCreate;
    }

    void setAutoCreate(AutoCreate autoCreate) {
        this.autoCreate = autoCreate;
    }

    /**
     * An auto create object
     *
     * @opensearch.internal
     */
    static class AutoCreate {
        private final boolean autoCreateIndex;
        private final List<Tuple<String, Boolean>> expressions;
        private final String string;

        private AutoCreate(String value) {
            boolean autoCreateIndex;
            List<Tuple<String, Boolean>> expressions = new ArrayList<>();
            try {
                autoCreateIndex = Booleans.parseBoolean(value);
            } catch (IllegalArgumentException ex) {
                try {
                    String[] patterns = Strings.commaDelimitedListToStringArray(value);
                    for (String pattern : patterns) {
                        if (pattern == null || pattern.trim().length() == 0) {
                            throw new IllegalArgumentException(
                                "Can't parse ["
                                    + value
                                    + "] for setting [action.auto_create_index] must "
                                    + "be either [true, false, or a comma separated list of index patterns]"
                            );
                        }
                        pattern = pattern.trim();
                        Tuple<String, Boolean> expression;
                        if (pattern.startsWith("-")) {
                            if (pattern.length() == 1) {
                                throw new IllegalArgumentException(
                                    "Can't parse ["
                                        + value
                                        + "] for setting [action.auto_create_index] "
                                        + "must contain an index name after [-]"
                                );
                            }
                            expression = new Tuple<>(pattern.substring(1), false);
                        } else if (pattern.startsWith("+")) {
                            if (pattern.length() == 1) {
                                throw new IllegalArgumentException(
                                    "Can't parse ["
                                        + value
                                        + "] for setting [action.auto_create_index] "
                                        + "must contain an index name after [+]"
                                );
                            }
                            expression = new Tuple<>(pattern.substring(1), true);
                        } else {
                            expression = new Tuple<>(pattern, true);
                        }
                        expressions.add(expression);
                    }
                    autoCreateIndex = true;
                } catch (IllegalArgumentException ex1) {
                    ex1.addSuppressed(ex);
                    throw ex1;
                }
            }
            this.expressions = expressions;
            this.autoCreateIndex = autoCreateIndex;
            this.string = value;
        }

        boolean isAutoCreateIndex() {
            return autoCreateIndex;
        }

        List<Tuple<String, Boolean>> getExpressions() {
            return expressions;
        }

        @Override
        public String toString() {
            return string;
        }
    }
}
