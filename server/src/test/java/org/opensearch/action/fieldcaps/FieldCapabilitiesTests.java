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

package org.opensearch.action.fieldcaps;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesTests extends AbstractSerializingTestCase<FieldCapabilities> {
    private static final String FIELD_NAME = "field";

    @Override
    protected FieldCapabilities doParseInstance(XContentParser parser) throws IOException {
        return FieldCapabilities.fromXContent(FIELD_NAME, parser);
    }

    @Override
    protected FieldCapabilities createTestInstance() {
        return randomFieldCaps(FIELD_NAME);
    }

    @Override
    protected Writeable.Reader<FieldCapabilities> instanceReader() {
        return FieldCapabilities::new;
    }

    public void testBuilder() {
        FieldCapabilities.Builder builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true, false, emptyList(), Collections.emptyMap());
        builder.add("index2", false, true, false, emptyList(), Collections.emptyMap());
        builder.add("index3", false, true, false, emptyList(), Collections.emptyMap());

        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertEquals(Collections.emptyMap(), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertEquals(Collections.emptyMap(), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, false, true, emptyList(), Collections.emptyMap());
        builder.add("index2", false, true, false, emptyList(), Collections.emptyMap());
        builder.add("index3", false, false, false, emptyList(), Collections.emptyMap());
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(false));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertNull(cap1.indices());
            assertThat(cap1.nonSearchableIndices(), equalTo(new String[] { "index1", "index3" }));
            assertThat(cap1.nonAggregatableIndices(), equalTo(new String[] { "index2", "index3" }));
            assertEquals(Collections.emptyMap(), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(false));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertThat(cap2.nonSearchableIndices(), equalTo(new String[] { "index1", "index3" }));
            assertThat(cap2.nonAggregatableIndices(), equalTo(new String[] { "index2", "index3" }));
            assertEquals(Collections.emptyMap(), cap2.meta());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true, true, emptyList(), Collections.emptyMap());
        builder.add("index2", false, true, true, Arrays.asList("aliasA", "aliasB"), Collections.singletonMap("foo", "bar"));
        builder.add("index3", false, true, true, Arrays.asList("aliasB", "aliasC"), Collections.singletonMap("foo", "quux"));
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(true));
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());
            assertEquals(Collections.singletonMap("foo", new HashSet<>(Arrays.asList("bar", "quux"))), cap1.meta());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(true));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[] { "index1", "index2", "index3" }));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
            assertEquals(Collections.singletonMap("foo", new HashSet<>(Arrays.asList("bar", "quux"))), cap2.meta());
        }
    }

    static FieldCapabilities randomFieldCaps(String fieldName) {
        String[] indices = null;
        if (randomBoolean()) {
            indices = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonSearchableIndices = null;
        if (randomBoolean()) {
            nonSearchableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonSearchableIndices.length; i++) {
                nonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonAggregatableIndices = null;
        if (randomBoolean()) {
            nonAggregatableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonAggregatableIndices.length; i++) {
                nonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }

        String[] aliasIndices = null;
        if (randomBoolean()) {
            aliasIndices = new String[randomIntBetween(0, 3)];
            for (int i = 0; i < aliasIndices.length; i++) {
                aliasIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }

        Map<String, Set<String>> meta;
        switch (randomInt(2)) {
            case 0:
                meta = Collections.emptyMap();
                break;
            case 1:
                meta = Collections.singletonMap("foo", Collections.singleton("bar"));
                break;
            default:
                meta = Collections.singletonMap("foo", new HashSet<>(Arrays.asList("bar", "baz")));
                break;
        }
        String[] aliases;
        switch (randomInt(2)) {
            case 0:
                aliases = new String[] {};
                break;
            case 1:
                aliases = Arrays.asList("foo", "bar").toArray(String[]::new);
                break;
            default:
                aliases = Arrays.asList("foo", "bar", "baz").toArray(String[]::new);
                break;
        }

        return new FieldCapabilities(
            fieldName,
            randomAlphaOfLengthBetween(5, 20),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            aliases,
            indices,
            nonSearchableIndices,
            nonAggregatableIndices,
            aliasIndices,
            meta
        );
    }

    @Override
    protected FieldCapabilities mutateInstance(FieldCapabilities instance) {
        String name = instance.getName();
        String type = instance.getType();
        boolean isAlias = instance.isAlias();
        boolean isSearchable = instance.isSearchable();
        boolean isAggregatable = instance.isAggregatable();
        String[] indices = instance.indices();
        String[] nonSearchableIndices = instance.nonSearchableIndices();
        String[] nonAggregatableIndices = instance.nonAggregatableIndices();
        String[] aliasIndices = instance.aliasesIndices();
        String[] aliases = instance.aliases();
        Map<String, Set<String>> meta = instance.meta();
        switch (between(0, 7)) {
            case 0:
                name += randomAlphaOfLengthBetween(1, 10);
                break;
            case 1:
                type += randomAlphaOfLengthBetween(1, 10);
                break;
            case 2:
                isSearchable = isSearchable == false;
                isAlias = randomBoolean();
                break;
            case 3:
                isAggregatable = isAggregatable == false;
                break;
            case 4:
                String[] newIndices;
                int startIndicesPos = 0;
                if (indices == null) {
                    newIndices = new String[between(1, 10)];
                } else {
                    newIndices = Arrays.copyOf(indices, indices.length + between(1, 10));
                    startIndicesPos = indices.length;
                }
                for (int i = startIndicesPos; i < newIndices.length; i++) {
                    newIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                indices = newIndices;
                break;
            case 5:
                String[] newNonSearchableIndices;
                int startNonSearchablePos = 0;
                if (nonSearchableIndices == null) {
                    newNonSearchableIndices = new String[between(1, 10)];
                } else {
                    newNonSearchableIndices = Arrays.copyOf(nonSearchableIndices, nonSearchableIndices.length + between(1, 10));
                    startNonSearchablePos = nonSearchableIndices.length;
                }
                for (int i = startNonSearchablePos; i < newNonSearchableIndices.length; i++) {
                    newNonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                nonSearchableIndices = newNonSearchableIndices;
                break;
            case 6:
                String[] newNonAggregatableIndices;
                int startNonAggregatablePos = 0;
                if (nonAggregatableIndices == null) {
                    newNonAggregatableIndices = new String[between(1, 10)];
                } else {
                    newNonAggregatableIndices = Arrays.copyOf(nonAggregatableIndices, nonAggregatableIndices.length + between(1, 10));
                    startNonAggregatablePos = nonAggregatableIndices.length;
                }
                for (int i = startNonAggregatablePos; i < newNonAggregatableIndices.length; i++) {
                    newNonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                nonAggregatableIndices = newNonAggregatableIndices;

                String[] newAliasIndices;
                int startAliasPos = 0;
                if (aliasIndices == null) {
                    newAliasIndices = new String[between(1, 10)];
                } else {
                    newAliasIndices = Arrays.copyOf(aliasIndices, aliasIndices.length + between(1, 10));
                    startAliasPos = aliasIndices.length;
                }
                for (int i = startAliasPos; i < newAliasIndices.length; i++) {
                    newAliasIndices[i] = randomAlphaOfLengthBetween(5, 20);
                }
                aliasIndices = newAliasIndices;
                break;
            case 7:
                Map<String, Set<String>> newMeta;
                if (meta.isEmpty()) {
                    newMeta = Collections.singletonMap("foo", Collections.singleton("bar"));
                } else {
                    newMeta = Collections.emptyMap();
                }
                meta = newMeta;

                String[] newAliases;
                if (Objects.isNull(aliases)) {
                    newAliases = List.of("foo").toArray(String[]::new);
                } else {
                    newAliases = new String[] {};
                }
                aliases = newAliases;
                break;
            default:
                throw new AssertionError();
        }
        return new FieldCapabilities(
            name,
            type,
            isAlias,
            isSearchable,
            isAggregatable,
            aliases,
            indices,
            nonSearchableIndices,
            nonAggregatableIndices,
            aliasIndices,
            meta
        );
    }
}
