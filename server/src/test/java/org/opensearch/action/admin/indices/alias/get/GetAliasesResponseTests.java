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

package org.opensearch.action.admin.indices.alias.get;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.AliasMetadata.Builder;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetAliasesResponseTests extends AbstractWireSerializingTestCase<GetAliasesResponse> {

    @Override
    protected GetAliasesResponse createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Writeable.Reader<GetAliasesResponse> instanceReader() {
        return GetAliasesResponse::new;
    }

    @Override
    protected GetAliasesResponse mutateInstance(GetAliasesResponse response) {
        return new GetAliasesResponse(mutateAliases(response.getAliases()));
    }

    private static Map<String, List<AliasMetadata>> mutateAliases(final Map<String, List<AliasMetadata>> aliases) {
        if (aliases.isEmpty()) {
            return createIndicesAliasesMap(1, 3);
        }

        if (randomBoolean()) {
            final Map<String, List<AliasMetadata>> builder = new HashMap<>(aliases);
            final Map<String, List<AliasMetadata>> list = createIndicesAliasesMap(1, 2);
            list.forEach((k, v) -> builder.put(k, v));
            return builder;
        }

        Set<String> indices = new HashSet<>();
        Iterator<String> keys = aliases.keySet().iterator();
        while (keys.hasNext()) {
            indices.add(keys.next());
        }

        List<String> indicesToBeModified = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        final Map<String, List<AliasMetadata>> builder = new HashMap<>();

        for (String index : indices) {
            List<AliasMetadata> list = new ArrayList<>(aliases.get(index));
            if (indicesToBeModified.contains(index)) {
                if (randomBoolean() || list.isEmpty()) {
                    list.add(createAliasMetadata());
                } else {
                    int aliasIndex = randomInt(list.size() - 1);
                    AliasMetadata aliasMetadata = list.get(aliasIndex);
                    list.add(aliasIndex, mutateAliasMetadata(aliasMetadata));
                }
            }
            builder.put(index, list);
        }
        return builder;
    }

    private static GetAliasesResponse createTestItem() {
        return new GetAliasesResponse(createIndicesAliasesMap(0, 5));
    }

    private static Map<String, List<AliasMetadata>> createIndicesAliasesMap(int min, int max) {
        final Map<String, List<AliasMetadata>> builder = new HashMap<>();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            List<AliasMetadata> aliasMetadata = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int alias = 0; alias < aliasesNum; alias++) {
                aliasMetadata.add(createAliasMetadata());
            }
            builder.put(index, aliasMetadata);
        }
        return builder;
    }

    public static AliasMetadata createAliasMetadata() {
        Builder builder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter("{\"term\":{\"year\":2016}}");
        }
        return builder.build();
    }

    public static AliasMetadata mutateAliasMetadata(AliasMetadata alias) {
        boolean changeAlias = randomBoolean();
        AliasMetadata.Builder builder = AliasMetadata.builder(changeAlias ? randomAlphaOfLengthBetween(2, 5) : alias.getAlias());
        builder.searchRouting(alias.searchRouting());
        builder.indexRouting(alias.indexRouting());
        builder.filter(alias.filter());

        if (false == changeAlias) {
            if (randomBoolean()) {
                builder.searchRouting(alias.searchRouting() + randomAlphaOfLengthBetween(1, 3));
            } else {
                builder.indexRouting(alias.indexRouting() + randomAlphaOfLengthBetween(1, 3));
            }
        }
        return builder.build();
    }
}
