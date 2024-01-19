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

package org.opensearch.index.alias;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.between;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLengthBetween;
import static org.opensearch.test.OpenSearchTestCase.randomBoolean;
import static org.opensearch.test.OpenSearchTestCase.randomFrom;
import static org.opensearch.test.OpenSearchTestCase.randomInt;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;

public final class RandomAliasActionsGenerator {

    private RandomAliasActionsGenerator() {}

    public static AliasActions randomAliasAction() {
        return randomAliasAction(false);
    }

    public static AliasActions randomAliasAction(boolean useStringAsFilter) {
        AliasActions action = new AliasActions(randomFrom(AliasActions.Type.values()));
        if (randomBoolean()) {
            action.index(randomAlphaOfLength(5));
        } else {
            int numIndices = randomIntBetween(1, 5);
            String[] indices = new String[numIndices];
            for (int i = 0; i < numIndices; i++) {
                indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
            }
            action.indices(indices);
        }
        if (action.actionType() != AliasActions.Type.REMOVE_INDEX) {
            if (randomBoolean()) {
                action.alias(randomAlphaOfLength(5));
            } else {
                int numAliases = randomIntBetween(1, 5);
                String[] aliases = new String[numAliases];
                for (int i = 0; i < numAliases; i++) {
                    aliases[i] = "alias-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
                }
                action.aliases(aliases);
            }
        }
        if (action.actionType() == AliasActions.Type.ADD) {
            if (randomBoolean()) {
                if (useStringAsFilter) {
                    action.filter(randomAlphaOfLength(5));
                } else {
                    action.filter(randomMap(randomInt(5)));
                }
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    action.routing(randomRouting().toString());
                } else {
                    action.searchRouting(randomRouting().toString());
                    action.indexRouting(randomRouting().toString());
                }
            }
            if (randomBoolean()) {
                action.writeIndex(randomBoolean());
            }
            if (randomBoolean()) {
                action.isHidden(randomBoolean());
            }
        }
        if (action.actionType() == AliasActions.Type.REMOVE) {
            if (randomBoolean()) {
                action.mustExist(randomBoolean());
            }
        }
        return action;
    }

    public static Map<String, Object> randomMap(int maxDepth) {
        int members = between(0, 5);
        Map<String, Object> result = new HashMap<>(members);
        for (int i = 0; i < members; i++) {
            Object value;
            switch (between(0, 3)) {
                case 0:
                    if (maxDepth > 0) {
                        value = randomMap(maxDepth - 1);
                    } else {
                        value = randomAlphaOfLength(5);
                    }
                    break;
                case 1:
                    value = randomAlphaOfLength(5);
                    break;
                case 2:
                    value = randomBoolean();
                    break;
                case 3:
                    value = randomLong();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            result.put(randomAlphaOfLength(5), value);
        }
        return result;
    }

    public static Object randomRouting() {
        return randomBoolean() ? randomAlphaOfLength(5) : randomInt();
    }
}
