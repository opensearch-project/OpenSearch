/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class SliceAssignmentWholeFilesHashTests extends OpenSearchTestCase {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long referenceMurmur3x64H1SeedZero(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long h1 = 0;
        long h2 = 0;
        int blocks = data.length / 16;
        for (int i = 0; i < blocks; i++) {
            long k1 = buffer.getLong(i * 16);
            long k2 = buffer.getLong(i * 16 + 8);
            h1 ^= Long.rotateLeft(k1 * C1, 31) * C2;
            h1 = (Long.rotateLeft(h1, 27) + h2) * 5 + 0x52dce729;
            h2 ^= Long.rotateLeft(k2 * C2, 33) * C1;
            h2 = (Long.rotateLeft(h2, 31) + h1) * 5 + 0x38495ab5;
        }
        int tail = blocks * 16;
        int remaining = data.length - tail;
        long k1 = 0;
        long k2 = 0;
        for (int i = remaining - 1; i >= 8; i--) {
            k2 ^= (data[tail + i] & 0xffL) << (8 * (i - 8));
        }
        for (int i = Math.min(remaining, 8) - 1; i >= 0; i--) {
            k1 ^= (data[tail + i] & 0xffL) << (8 * i);
        }
        if (remaining > 8) {
            h2 ^= Long.rotateLeft(k2 * C2, 33) * C1;
        }
        if (remaining > 0) {
            h1 ^= Long.rotateLeft(k1 * C1, 31) * C2;
        }
        h1 ^= data.length;
        h2 ^= data.length;
        h1 += h2;
        h2 += h1;
        h1 = fmix(h1);
        h2 = fmix(h2);
        return h1 + h2;
    }

    private static int expectedOwner(String repositoryRelativeKey, int numShards) {
        long h1 = referenceMurmur3x64H1SeedZero(repositoryRelativeKey.getBytes(StandardCharsets.UTF_8));
        return (int) Math.floorMod(h1, (long) numShards);
    }

    private static ObjectStoreSourceConfig wholeFilesConfig(String sourcePrefix) {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("shard_split", "whole_files");
        params.put("source_prefix", sourcePrefix);
        return new ObjectStoreSourceConfig(params);
    }

    private static ObjectStoreManifest containerRelative(ObjectStoreSourceConfig config, List<String> repositoryRelativeKeys) {
        Map<String, Long> entries = new LinkedHashMap<>();
        String directoryPrefix = RepositoryBlobContainerResolver.directoryPrefix(config);
        for (String key : repositoryRelativeKeys) {
            entries.put(RepositoryBlobContainerResolver.containerRelativeKey(directoryPrefix, key), 1L);
        }
        return new ObjectStoreManifest(entries);
    }

    private static List<String> ownedNames(ObjectStoreSourceConfig config, ObjectStoreManifest manifest, int shardId, int numShards)
        throws Exception {
        List<String> names = new ArrayList<>();
        for (ObjectSlice slice : SliceAssignment.assign(config, manifest, null, null, shardId, numShards)) {
            names.add(slice.getObjectName());
        }
        return names;
    }

    public void testReferenceMurmur3MatchesThePinnedGoldenValueAndTheProductionHash() {
        byte[] key = "dump/2024/a.ndjson".getBytes(StandardCharsets.UTF_8);

        assertThat(referenceMurmur3x64H1SeedZero(key), is(0x656d7870dafd51eeL));
        assertThat(MurmurHash3.hash128(key, 0, key.length, 0L, new MurmurHash3.Hash128()).h1, is(0x656d7870dafd51eeL));
        assertThat(referenceMurmur3x64H1SeedZero("a.ndjson".getBytes(StandardCharsets.UTF_8)), is(0x5b4f24b9e7baafe4L));
        byte[] multiBlockKey = "dump/2024/longer-key.ndjson".getBytes(StandardCharsets.UTF_8);
        assertThat(
            referenceMurmur3x64H1SeedZero(multiBlockKey),
            is(MurmurHash3.hash128(multiBlockKey, 0, multiBlockKey.length, 0L, new MurmurHash3.Hash128()).h1)
        );
    }

    public void testWholeFilesHashesTheRepositoryRelativeKeyUnderADirectoryBearingSourcePrefix() throws Exception {
        ObjectStoreSourceConfig config = wholeFilesConfig("dump/2024/");
        List<String> keys = List.of("dump/2024/a.ndjson", "dump/2024/b.ndjson", "dump/2024/c.ndjson", "dump/2024/d.ndjson");
        ObjectStoreManifest manifest = containerRelative(config, keys);

        assertThat(ownedNames(config, manifest, 0, 4), contains("b.ndjson"));
        assertThat(ownedNames(config, manifest, 1, 4), contains("d.ndjson"));
        assertThat(ownedNames(config, manifest, 2, 4), contains("a.ndjson"));
        assertThat(ownedNames(config, manifest, 3, 4), contains("c.ndjson"));
        for (String key : keys) {
            String name = key.substring("dump/2024/".length());
            assertThat(key, ownedNames(config, manifest, expectedOwner(key, 4), 4).contains(name), is(true));
        }
    }

    public void testWholeFilesHashesTheRepositoryRelativeKeyWhenTheSourcePrefixHasALeaf() throws Exception {
        ObjectStoreSourceConfig config = wholeFilesConfig("dump/2024/part-");
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            keys.add("dump/2024/part-" + i + ".ndjson");
        }
        ObjectStoreManifest manifest = containerRelative(config, keys);
        int numShards = 3;

        for (int shard = 0; shard < numShards; shard++) {
            List<String> expected = new ArrayList<>();
            for (String key : manifest.asSortedMap().keySet()) {
                if (expectedOwner("dump/2024/" + key, numShards) == shard) {
                    expected.add(key);
                }
            }
            assertThat(ownedNames(config, manifest, shard, numShards), is(expected));
        }
    }

    public void testWholeFilesWithoutADirectoryHashesTheKeyAsWritten() throws Exception {
        ObjectStoreSourceConfig config = wholeFilesConfig("part-");
        ObjectStoreManifest manifest = containerRelative(config, List.of("part-a.ndjson"));
        int owner = expectedOwner("part-a.ndjson", 5);

        for (int shard = 0; shard < 5; shard++) {
            if (shard == owner) {
                assertThat(ownedNames(config, manifest, shard, 5), contains("part-a.ndjson"));
            } else {
                assertThat(ownedNames(config, manifest, shard, 5), is(empty()));
            }
        }
    }
}
