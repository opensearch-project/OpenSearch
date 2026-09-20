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

package org.opensearch.indices;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class SystemIndexDescriptorTests extends OpenSearchTestCase {

    public void testValidation() {
        {
            Exception ex = expectThrows(NullPointerException.class, () -> new SystemIndexDescriptor(null, randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new SystemIndexDescriptor("", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new SystemIndexDescriptor(".", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class, () -> new SystemIndexDescriptor(".*", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*" + randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
    }

    public void testEqualsAndHashCode() {
        UnrestrictedSystemIndexDescriptor descriptor1 = new UnrestrictedSystemIndexDescriptor(".test-index", "desc1");
        SystemIndexDescriptor descriptor2 = new SystemIndexDescriptor(".test-index", "desc2");
        SystemIndexDescriptor descriptor3 = new SystemIndexDescriptor(".other-index", "desc1");

        // Same pattern means equal, regardless of description or type
        assertEquals(descriptor1, descriptor2);
        assertEquals(descriptor1.hashCode(), descriptor2.hashCode());

        // Different pattern means not equal
        assertNotEquals(descriptor1, descriptor3);
    }

    public void testToString() {
        UnrestrictedSystemIndexDescriptor descriptor = new UnrestrictedSystemIndexDescriptor(".test-index", "test description");
        String str = descriptor.toString();
        assertThat(str, containsString(".test-index"));
        assertThat(str, containsString("test description"));
    }

    public void testConcreteIndexMappings() {
        String mappings = "{\"_meta\":{\"schema_version\":3},\"properties\":{}}";
        SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder(".test-*", "test description")
            .setMappings(".test-index", mappings)
            .build();

        assertTrue(descriptor.hasMappings());
        assertEquals(".test-index", descriptor.getPrimaryIndex());
        assertThat(descriptor.getWriteAlias(), nullValue());
        assertEquals(mappings, descriptor.getMappings());
        assertEquals(3L, descriptor.getMappingVersion());
    }

    public void testWriteAliasMappings() {
        String mappings = "{\"_meta\":{\"schema_version\":7},\"properties\":{}}";
        SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder(".test-history-*", "test description")
            .setMappingsForWriteAlias(".test-history-write", mappings)
            .build();

        assertTrue(descriptor.hasMappings());
        assertThat(descriptor.getPrimaryIndex(), nullValue());
        assertEquals(".test-history-write", descriptor.getWriteAlias());
        assertEquals(7L, descriptor.getMappingVersion());
    }

    public void testDescriptorWithoutMappings() {
        SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder(".test-index", "test description").build();

        assertFalse(descriptor.hasMappings());
        assertThat(descriptor.getPrimaryIndex(), nullValue());
        assertThat(descriptor.getWriteAlias(), nullValue());
        assertThat(descriptor.getMappings(), nullValue());
        assertEquals(SystemIndexMappingUpdater.NO_SCHEMA_VERSION, descriptor.getMappingVersion());
    }

    public void testMappingValidation() {
        Exception missingVersion = expectThrows(
            IllegalArgumentException.class,
            () -> SystemIndexDescriptor.builder(".test-index", "test").setMappings(".test-index", "{\"properties\":{}}").build()
        );
        assertThat(missingVersion.getMessage(), containsString("must contain [_meta.schema_version]"));

        Exception invalidVersion = expectThrows(
            IllegalArgumentException.class,
            () -> SystemIndexDescriptor.builder(".test-index", "test")
                .setMappings(".test-index", "{\"_meta\":{\"schema_version\":\"one\"}}")
                .build()
        );
        assertThat(invalidVersion.getMessage(), containsString("must be a number"));

        Exception negativeVersion = expectThrows(
            IllegalArgumentException.class,
            () -> SystemIndexDescriptor.builder(".test-index", "test")
                .setMappings(".test-index", "{\"_meta\":{\"schema_version\":-1}}")
                .build()
        );
        assertThat(negativeVersion.getMessage(), containsString("must be a non-negative integer"));

        Exception mismatchedIndex = expectThrows(
            IllegalArgumentException.class,
            () -> SystemIndexDescriptor.builder(".test-*", "test").setMappings(".other-index", "{\"_meta\":{\"schema_version\":1}}").build()
        );
        assertThat(mismatchedIndex.getMessage(), containsString("must match system index pattern"));
    }

    public void testMappingTargetCanOnlyBeConfiguredOnce() {
        SystemIndexDescriptor.Builder builder = SystemIndexDescriptor.builder(".test-*", "test")
            .setMappings(".test-index", "{\"_meta\":{\"schema_version\":1}}");

        Exception exception = expectThrows(
            IllegalStateException.class,
            () -> builder.setMappingsForWriteAlias(".test-write", "{\"_meta\":{\"schema_version\":1}}")
        );
        assertThat(exception.getMessage(), containsString("mapping target is already configured"));
    }
}
