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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link ShardSplit} parsing, naming, and string representation.
 */
public class ShardSplitTests extends OpenSearchTestCase {

    public void testGetName() {
        assertThat(ShardSplit.EQUAL_BYTES.getName(), equalTo("equal_bytes"));
        assertThat(ShardSplit.WHOLE_FILES.getName(), equalTo("whole_files"));
        assertThat(ShardSplit.SHARD_PREFIXES.getName(), equalTo("shard_prefixes"));
    }

    public void testToStringMatchesName() {
        assertThat(ShardSplit.WHOLE_FILES.toString(), equalTo("whole_files"));
    }

    public void testFromStringCaseInsensitive() {
        assertThat(ShardSplit.fromString("EQUAL_BYTES"), is(ShardSplit.EQUAL_BYTES));
        assertThat(ShardSplit.fromString("whole_files"), is(ShardSplit.WHOLE_FILES));
        assertThat(ShardSplit.fromString("Shard_Prefixes"), is(ShardSplit.SHARD_PREFIXES));
    }

    public void testFromStringNullThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ShardSplit.fromString(null));
        assertThat(e.getMessage(), containsString("shard_split is required"));
    }

    public void testFromStringUnknownThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ShardSplit.fromString("nope"));
        assertThat(e.getMessage(), containsString("Unknown shard_split [nope]"));
    }
}
