/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.repositories.RepositoriesService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Factory for creating {@link ObjectStoreShardConsumer} instances and parsing checkpoints. Implements the
 * non-deprecated {@link #createShardConsumer(String, int, IndexMetadata)} entry point. On creation it validates the
 * configuration, resolves the backing {@link BlobContainer} from the registered repository named in the
 * configuration, parses the explicit {@code manifest} parameter with {@link ManifestParser}, translates its
 * repository-relative keys to container-relative names, and constructs a shard consumer. Under
 * {@code discovery=list} there is no manifest to parse and an empty one is passed through.
 *
 * <p>Failures are split by whether retrying can help. A deterministic configuration error (a rejected or malformed
 * parameter or manifest, a registered repository that is not a read-only blob-store repository, an
 * {@code equal_bytes} object whose cut cannot be resolved because it is not splittable or was truncated, or a discovery
 * that exceeds {@code max_objects}, the directory depth limit, or {@code max_list_requests}) yields an
 * {@link ObjectStoreConfigErrorConsumer}, so the poller pauses ingestion on the first poll instead of retrying creation
 * forever. Because the poller then holds a consumer, a later update of the source parameters rebuilds it, so pointing
 * {@code repository} at a usable repository takes effect. A failure that may be transient, such as an unregistered
 * repository or an IO error reaching the object store, is still thrown so that the poller retries creation.
 */
public class ObjectStoreConsumerFactory
    implements
        IngestionConsumerFactory<IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage>, ObjectStorePointer> {

    private final Supplier<RepositoriesService> repositoriesServiceSupplier;

    /**
     * Creates an object store consumer factory.
     *
     * @param repositoriesServiceSupplier supplier of the repositories service used to resolve the backing repository;
     *                                    dereferenced lazily at shard-consumer creation time
     */
    public ObjectStoreConsumerFactory(Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.repositoriesServiceSupplier = Objects.requireNonNull(
            repositoriesServiceSupplier,
            "repositoriesServiceSupplier must not be null"
        );
    }

    @Override
    public IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> createShardConsumer(
        String clientId,
        int shardId,
        IndexMetadata indexMetadata
    ) {
        int numShards = indexMetadata.getNumberOfShards();
        final ObjectStoreSourceConfig config;
        final ObjectStoreManifest manifest;
        final String resolvedPrefix;
        try {
            config = new ObjectStoreSourceConfig(indexMetadata.getIngestionSource().params());
            ObjectStoreManifest repositoryManifest = config.getDiscovery() == Discovery.LIST
                ? ObjectStoreManifest.EMPTY
                : ManifestParser.parse(config.getManifestJson(), config.getMaxObjects());
            ObjectStoreSourceConfigValidator.validate(config, repositoryManifest);
            StreamPoller.ResetState resetState = pointerResetType(indexMetadata);
            ObjectStoreSourceConfigValidator.validatePointerReset(resetState);
            ObjectStoreSourceConfigValidator.validatePointerResetForShards(resetState, config.getShardSplit(), numShards);
            ObjectStoreSourceConfigValidator.validateMapper(indexMetadata.getIngestionSource().getMapperType());
            manifest = toContainerRelative(config, repositoryManifest);
            resolvedPrefix = config.getShardSplit() == ShardSplit.EQUAL_BYTES ? null : RepositoryBlobContainerResolver.prefix(config);
        } catch (IllegalArgumentException | OpenSearchParseException e) {
            return new ObjectStoreConfigErrorConsumer(shardId, e);
        }
        RepositoriesService service = repositoriesServiceSupplier.get();
        final BlobContainer container;
        try {
            container = RepositoryBlobContainerResolver.resolveContainer(service, config);
        } catch (UnusableRepositoryException e) {
            return new ObjectStoreConfigErrorConsumer(shardId, e);
        }
        final List<ObjectSlice> slices;
        final StoreReadTally spent = new StoreReadTally();
        try {
            slices = SliceAssignment.assign(config, manifest, container, resolvedPrefix, shardId, numShards, spent);
        } catch (IllegalArgumentException | CostLimitExceededException | UnresolvableBoundaryException e) {
            return new ObjectStoreConfigErrorConsumer(shardId, e);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to resolve slices for shard " + shardId, e);
        }
        try {
            requireWithinCostCaps(config, spent);
        } catch (CostLimitExceededException e) {
            return new ObjectStoreConfigErrorConsumer(shardId, e);
        }
        return new ObjectStoreShardConsumer(config, container, shardId, slices, spent);
    }

    private static ObjectStoreManifest toContainerRelative(ObjectStoreSourceConfig config, ObjectStoreManifest repositoryManifest) {
        String directoryPrefix = RepositoryBlobContainerResolver.directoryPrefix(config);
        Map<String, Long> containerRelative = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : repositoryManifest.asSortedMap().entrySet()) {
            containerRelative.put(RepositoryBlobContainerResolver.containerRelativeKey(directoryPrefix, entry.getKey()), entry.getValue());
        }
        return new ObjectStoreManifest(containerRelative);
    }

    @Override
    public ObjectStorePointer parsePointerFromString(String pointer) {
        return ObjectStorePointer.fromString(pointer);
    }

    private static StreamPoller.ResetState pointerResetType(IndexMetadata indexMetadata) {
        IngestionSource.PointerInitReset reset = indexMetadata.getIngestionSource().getPointerInitReset();
        return reset == null ? null : reset.getType();
    }

    /**
     * Fails the shard when resolving its slices already spent more than its cost caps allow. Boundary resolution runs
     * before the consumer exists and may read up to one record's worth of bytes per cut, so a cap tight enough to be
     * exceeded by assignment alone is a configuration error rather than a transient read failure, and reporting it here
     * pauses the shard with that reason instead of surfacing it later as an object-store IO error.
     *
     * @param config the parsed source configuration
     * @param spent  the get requests and bytes slice assignment spent
     * @throws CostLimitExceededException if assignment alone exceeded either cap
     */
    private static void requireWithinCostCaps(final ObjectStoreSourceConfig config, final StoreReadTally spent)
        throws CostLimitExceededException {
        if (spent.getRequests() > config.getMaxGetRequests()) {
            throw new CostLimitExceededException(
                "resolving slice boundaries issued ["
                    + spent.getRequests()
                    + "] get requests, which exceeds ["
                    + ObjectStoreSourceConfig.MAX_GET_REQUESTS
                    + "="
                    + config.getMaxGetRequests()
                    + "]; raise the cap or use a shard split that needs no boundary resolution"
            );
        }
        if (spent.bytesRead() > config.getMaxBytes()) {
            throw new CostLimitExceededException(
                "resolving slice boundaries read ["
                    + spent.bytesRead()
                    + "] bytes, which exceeds ["
                    + ObjectStoreSourceConfig.MAX_BYTES
                    + "="
                    + config.getMaxBytes()
                    + "]; raise the cap or use a shard split that needs no boundary resolution"
            );
        }
    }
}
