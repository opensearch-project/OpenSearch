/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link Discovery} parsing and naming.
 */
public class DiscoveryTests extends OpenSearchTestCase {

    public void testNullUsesTheDeclaredDefault() {
        assertThat(Discovery.fromString(null), is(ObjectStoreSourceConfig.DEFAULT_DISCOVERY));
    }

    public void testNullDefaultsToManifest() {
        assertThat(Discovery.fromString(null), is(Discovery.MANIFEST));
    }

    public void testParsesBothModes() {
        assertThat(Discovery.fromString("manifest"), is(Discovery.MANIFEST));
        assertThat(Discovery.fromString("list"), is(Discovery.LIST));
    }

    public void testParsingIsCaseInsensitive() {
        assertThat(Discovery.fromString("MANIFEST"), is(Discovery.MANIFEST));
        assertThat(Discovery.fromString("List"), is(Discovery.LIST));
    }

    public void testUnknownValueRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Discovery.fromString("auto"));
        assertThat(e.getMessage(), containsString("Unknown discovery [auto]"));
        assertThat(e.getMessage(), containsString("[manifest, list]"));
    }

    public void testNames() {
        assertThat(Discovery.MANIFEST.getName(), is("manifest"));
        assertThat(Discovery.LIST.getName(), is("list"));
        assertThat(Discovery.MANIFEST.toString(), is("manifest"));
        assertThat(Discovery.LIST.toString(), is("list"));
    }
}
