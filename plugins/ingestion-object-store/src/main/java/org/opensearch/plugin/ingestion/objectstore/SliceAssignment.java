/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IngestionSource.SourcePartitionStrategy;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.indices.pollingingest.SourcePartitionAssignment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Computes the disjoint set of {@link ObjectSlice}s a shard owns from the frozen manifest, for each
 * {@link ShardSplit} strategy. Every record in the source is owned by exactly one shard.
 *
 * <ul>
 *   <li>{@link ShardSplit#EQUAL_BYTES}: the single object is divided into {@code numShards} contiguous byte ranges
 *       (SIMPLE assignment), each resolved to line boundaries via {@link BoundaryResolver} so ranges meet exactly at
 *       line starts.</li>
 *   <li>{@link ShardSplit#WHOLE_FILES}: objects are distributed across shards by the pinned hash murmur3_128
 *       (x64, seed 0) over the UTF-8 bytes of the repository-relative manifest key, modulo the shard count; a shard
 *       owns the objects that hash to it. When there are fewer objects than shards the extra shards own nothing.</li>
 *   <li>{@link ShardSplit#SHARD_PREFIXES}: a shard owns the objects under the sub-prefix
 *       {@code <resolvedPrefix>/<shardId>/} (SIMPLE assignment). An object that falls under no shard's sub-prefix is
 *       owned by no shard; this is logged as a warning (once, from shard 0) rather than being dropped silently.</li>
 * </ul>
 *
 * <p>All three read the object set from the frozen manifest. Under {@link Discovery#LIST} the manifest is bypassed and
 * the shard discovers its own objects with {@link ListDiscovery}.
 */
final class SliceAssignment {

    private static final Logger logger = LogManager.getLogger(SliceAssignment.class);

    private SliceAssignment() {}

    /**
     * Computes the slices owned by a shard.
     *
     * @param config         the source configuration
     * @param manifest       the frozen manifest
     * @param container      the blob container (used to resolve line boundaries for {@code equal_bytes})
     * @param resolvedPrefix the container-relative prefix (used only for {@code shard_prefixes}; may be {@code null})
     * @param shardId        the shard id
     * @param numShards      the total number of shards
     * @return the ordered, disjoint slices this shard owns (possibly empty)
     * @throws IOException if resolving a line boundary fails
     */
    /**
     * Resolves the ordered, disjoint slices a shard owns.
     *
     * @param config         the parsed source configuration
     * @param manifest       the frozen manifest, container-relative
     * @param container      the blob container to resolve boundaries against
     * @param resolvedPrefix the container-relative prefix, or {@code null} when a single object is configured
     * @param shardId        the shard whose slices are wanted
     * @param numShards      the index's shard count
     * @return the ordered, disjoint slices this shard owns
     * @throws IOException if a boundary cannot be resolved
     */
    public static List<ObjectSlice> assign(
        ObjectStoreSourceConfig config,
        ObjectStoreManifest manifest,
        BlobContainer container,
        String resolvedPrefix,
        int shardId,
        int numShards
    ) throws IOException {
        return assign(config, manifest, container, resolvedPrefix, shardId, numShards, new StoreReadTally());
    }

    /**
     * Resolves the ordered, disjoint slices a shard owns, charging any boundary read to the given tally so a cost cap
     * can account for work done before the reading consumer exists.
     *
     * @param config         the parsed source configuration
     * @param manifest       the frozen manifest, container-relative
     * @param container      the blob container to resolve boundaries against
     * @param resolvedPrefix the container-relative prefix, or {@code null} when a single object is configured
     * @param shardId        the shard whose slices are wanted
     * @param numShards      the index's shard count
     * @param tally          collects the get requests and bytes boundary resolution spends
     * @return the ordered, disjoint slices this shard owns
     * @throws IOException if a boundary cannot be resolved
     */
    public static List<ObjectSlice> assign(
        ObjectStoreSourceConfig config,
        ObjectStoreManifest manifest,
        BlobContainer container,
        String resolvedPrefix,
        int shardId,
        int numShards,
        StoreReadTally tally
    ) throws IOException {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(tally, "tally must not be null");
        Objects.requireNonNull(manifest, "manifest must not be null");
        if (shardId < 0 || shardId >= numShards) {
            throw new IllegalArgumentException("shardId [" + shardId + "] must be in [0, numShards=" + numShards + ")");
        }
        if (config.getDiscovery() == Discovery.LIST) {
            return assignByListing(config, container, resolvedPrefix, shardId, numShards);
        }
        return switch (config.getShardSplit()) {
            case EQUAL_BYTES -> assignEqualBytes(manifest, container, shardId, numShards, config.getMaxRecordBytes(), tally);
            case WHOLE_FILES -> assignWholeFiles(manifest, RepositoryBlobContainerResolver.directoryPrefix(config), shardId, numShards);
            case SHARD_PREFIXES -> assignShardPrefixes(manifest, resolvedPrefix, shardId, numShards);
        };
    }

    private static List<ObjectSlice> assignByListing(
        ObjectStoreSourceConfig config,
        BlobContainer container,
        String resolvedPrefix,
        int shardId,
        int numShards
    ) throws IOException {
        String subPrefix = subPrefixFor(resolvedPrefix, simplePartition(shardId, numShards));
        List<ObjectSlice> slices = ListDiscovery.discover(container, subPrefix, config.getMaxObjects(), config.getMaxListRequests());
        logger.info("shard {} discovered {} objects under sub-prefix [{}]", shardId, slices.size(), LogSanitizer.sanitize(subPrefix));
        return slices;
    }

    private static int simplePartition(int shardId, int numShards) {
        return SourcePartitionAssignment.assignSourcePartitions(shardId, numShards, numShards, SourcePartitionStrategy.SIMPLE).get(0);
    }

    private static List<ObjectSlice> assignEqualBytes(
        ObjectStoreManifest manifest,
        BlobContainer container,
        int shardId,
        int numShards,
        long maxRecordBytes,
        StoreReadTally tally
    ) throws IOException {
        if (manifest.objectCount() != 1) {
            throw new IllegalArgumentException("equal_bytes requires exactly one object in the manifest, found " + manifest.objectCount());
        }
        Objects.requireNonNull(container, "container must not be null for equal_bytes");
        int partition = simplePartition(shardId, numShards);
        String name = manifest.asSortedMap().firstKey();
        long size = manifest.sizeOf(name);
        long nominalStart = floorScaled(size, partition, numShards);
        long nominalEnd = (partition == numShards - 1) ? size : floorScaled(size, partition + 1, numShards);
        long maxScanBytes = boundaryScanBytes(maxRecordBytes);
        long resolvedStart = BoundaryResolver.resolveLineStart(container, name, nominalStart, size, maxScanBytes, tally);
        long resolvedEnd = (partition == numShards - 1)
            ? size
            : BoundaryResolver.resolveLineStart(container, name, nominalEnd, size, maxScanBytes, tally);
        if (resolvedStart >= resolvedEnd) {
            return List.of();
        }
        return List.of(new ObjectSlice(name, resolvedStart, resolvedEnd, size));
    }

    /**
     * Returns how many bytes {@link BoundaryResolver} may scan from a nominal cut to find the newline that ends the line
     * straddling or starting at that cut, such that every line {@link NdjsonFramer} accepts can be resolved.
     *
     * <p>The scan must include the terminating {@code \n} itself. A cut exactly at the start of a line at offset
     * {@code > 0} needs the whole line plus its newline: {@code maxLineBytes(maxRecordBytes, false) + 1}, that is the
     * payload, a trailing {@code \r} and the {@code \n}. A cut inside the first line (any offset {@code >= 1}) needs at
     * most that line's length, which may also carry a three-byte UTF-8 BOM: {@code maxLineBytes(maxRecordBytes, true)}.
     * The BOM allowance exceeds the one newline byte, so the second bound covers both cases, and it saturates rather
     * than overflowing. The bound only decides whether a cut can be resolved at all; it never moves the resolved line
     * start, so which shard owns which line is unchanged.
     *
     * @param maxRecordBytes the per-record payload byte cap
     * @return the maximum number of bytes to scan for a newline from a nominal cut
     */
    private static long boundaryScanBytes(long maxRecordBytes) {
        return NdjsonFramer.maxLineBytes(maxRecordBytes, true);
    }

    /**
     * Computes {@code floor(size * k / numShards)} without overflowing {@code long} for very large declared sizes, by
     * splitting the product into a quotient term and a bounded remainder term.
     *
     * @param size      the object size in bytes
     * @param k         the partition multiplier (in {@code [0, numShards]})
     * @param numShards the shard count
     * @return {@code floor(size * k / numShards)}
     */
    private static long floorScaled(long size, long k, long numShards) {
        return size / numShards * k + (size % numShards) * k / numShards;
    }

    /**
     * Assigns whole objects by the pinned hash of their repository-relative manifest key. The manifest handed to
     * {@link #assign} is container-relative (the configured directory prefix has been stripped so the names can be read
     * from the container), so the directory prefix is put back before hashing. The validator requires every
     * {@code whole_files} manifest key to start with {@code source_prefix}, and {@code source_prefix} has no empty or
     * leading segment, so the directory prefix plus the container-relative name is exactly the key the user wrote.
     *
     * @param manifest        the container-relative manifest
     * @param directoryPrefix the configured directory prefix stripped from each manifest key, or the empty string
     * @param shardId         the shard id
     * @param numShards       the total number of shards
     * @return the slices this shard owns, named container-relative
     */
    private static List<ObjectSlice> assignWholeFiles(ObjectStoreManifest manifest, String directoryPrefix, int shardId, int numShards) {
        List<ObjectSlice> slices = new ArrayList<>();
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        for (Map.Entry<String, Long> entry : manifest.asSortedMap().entrySet()) {
            if (ownerShard(directoryPrefix + entry.getKey(), numShards, scratch) == shardId) {
                slices.add(new ObjectSlice(entry.getKey(), 0, entry.getValue(), entry.getValue()));
            }
        }
        return slices;
    }

    /**
     * Computes the owning shard for an object key using the pinned hash: murmur3_128 (x64, seed 0) over the UTF-8
     * bytes of the full repository-relative manifest key, reduced modulo the shard count. This mapping is deterministic and recomputable so that
     * every object is owned by exactly one shard.
     *
     * @param key       the repository-relative object key, as written in the manifest
     * @param numShards the total number of shards
     * @param scratch   a reusable hash holder, so scanning a large manifest does not allocate one per key
     * @return the owning shard id
     */
    private static int ownerShard(String key, int numShards, MurmurHash3.Hash128 scratch) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(keyBytes, 0, keyBytes.length, 0L, scratch);
        return (int) Math.floorMod(hash.h1, (long) numShards);
    }

    private static List<ObjectSlice> assignShardPrefixes(ObjectStoreManifest manifest, String resolvedPrefix, int shardId, int numShards) {
        int partition = simplePartition(shardId, numShards);
        String subPrefix = subPrefixFor(resolvedPrefix, partition);
        List<ObjectSlice> slices = new ArrayList<>();
        String[] allSubPrefixes = partition == 0 ? subPrefixesFor(resolvedPrefix, numShards) : null;
        long unowned = 0;
        String firstUnowned = null;
        for (Map.Entry<String, Long> entry : manifest.asSortedMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(subPrefix)) {
                slices.add(new ObjectSlice(key, 0, entry.getValue(), entry.getValue()));
            } else if (allSubPrefixes != null && ownedByNoShard(allSubPrefixes, key)) {
                unowned++;
                if (firstUnowned == null) {
                    firstUnowned = key;
                }
            }
        }
        if (unowned > 0) {
            logger.warn(
                "shard_prefixes: [{}] object(s), the first being [{}], are not under any shard sub-prefix "
                    + "<prefix>/<0..{}>/ and will not be ingested by any shard",
                unowned,
                LogSanitizer.sanitize(firstUnowned),
                numShards - 1
            );
        }
        return slices;
    }

    private static String[] subPrefixesFor(String resolvedPrefix, int numShards) {
        String base = normalizedBase(resolvedPrefix);
        String[] subPrefixes = new String[numShards];
        for (int shard = 0; shard < numShards; shard++) {
            subPrefixes[shard] = subPrefixFor(base, shard);
        }
        return subPrefixes;
    }

    private static boolean ownedByNoShard(String[] subPrefixes, String key) {
        for (String subPrefix : subPrefixes) {
            if (key.startsWith(subPrefix)) {
                return false;
            }
        }
        return true;
    }

    private static String subPrefixFor(String resolvedPrefix, int shardId) {
        return normalizedBase(resolvedPrefix) + shardId + "/";
    }

    private static String normalizedBase(String resolvedPrefix) {
        if (resolvedPrefix == null || resolvedPrefix.isEmpty()) {
            return "";
        }
        return resolvedPrefix.endsWith("/") ? resolvedPrefix : resolvedPrefix + "/";
    }
}
