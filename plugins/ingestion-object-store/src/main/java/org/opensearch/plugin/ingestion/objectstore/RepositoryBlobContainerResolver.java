/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Resolves the {@link BlobContainer} backing an {@link ObjectStoreSourceConfig} by referencing a registered
 * OpenSearch repository through the {@link RepositoriesService}. The repository named by
 * {@link ObjectStoreSourceConfig#getRepository()} is looked up and its {@link BlobStoreRepository#blobStore()} is
 * used to obtain a container rooted at the repository base path plus the directory portion of the configured
 * {@code source_url} or {@code source_prefix}. The repository must be registered with {@code "readonly": true}, so
 * that a writable repository such as the cluster's snapshot repository cannot be read through an ingestion source.
 *
 * <p>The configured path is split into leading directory segments plus a leaf. The returned container is rooted at
 * the directory segments, and {@link #prefix(ObjectStoreSourceConfig)} returns the leaf. This keeps blob names and
 * prefixes rooted at the directory so ranged
 * {@link BlobContainer#readBlob(String, long, long)} calls work identically on directory-based backends (fs) and
 * flat key-value backends (s3). No listing is performed; the object set comes from the frozen manifest.
 */
final class RepositoryBlobContainerResolver {

    private static final String SEPARATOR = "/";

    private RepositoryBlobContainerResolver() {}

    /**
     * Resolves the {@link BlobContainer} rooted at the directory portion of the configured path within the named
     * repository's blob store.
     *
     * @param service the repositories service used to look up the registered repository; must not be {@code null}
     * @param config  the parsed source configuration; must not be {@code null}
     * @return a blob container rooted at the repository base path plus the configured directory segments
     * @throws IllegalArgumentException     if the repository is not registered
     * @throws UnusableRepositoryException if the repository is not a blob-store repository or is not read-only
     */
    public static BlobContainer resolveContainer(final RepositoriesService service, final ObjectStoreSourceConfig config) {
        Objects.requireNonNull(service, "service must not be null");
        Objects.requireNonNull(config, "config must not be null");
        final String repositoryName = config.getRepository();
        final Repository repository;
        try {
            repository = service.repository(repositoryName);
        } catch (final RepositoryMissingException e) {
            throw new IllegalArgumentException(
                "object_store repository [" + repositoryName + "] is not registered; register it before creating the index",
                e
            );
        }
        if ((repository instanceof BlobStoreRepository) == false) {
            throw new UnusableRepositoryException(
                "object_store repository ["
                    + repositoryName
                    + "] is of type ["
                    + repository.getMetadata().type()
                    + "] which is not a blob-store repository; use an s3, gcs, azure, or fs repository"
            );
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        if (blobStoreRepository.isReadOnly() == false) {
            throw new UnusableRepositoryException(
                "object_store repository ["
                    + repositoryName
                    + "] is not read-only; register a dedicated repository with \"readonly\": true for ingestion"
            );
        }
        BlobPath path = blobStoreRepository.basePath();
        final List<String> directorySegments = directorySegments(configuredPath(config));
        if (directorySegments.isEmpty() == false) {
            path = path.add(directorySegments);
        }
        return blobStoreRepository.blobStore().blobContainer(path);
    }

    /**
     * Returns the leaf prefix of the configured {@code source_prefix}, relative to the container returned by
     * {@link #resolveContainer(RepositoriesService, ObjectStoreSourceConfig)}. A prefix ending in {@code /} yields an
     * empty leaf, matching every blob directly under the directory.
     *
     * <p>This deliberately reads {@code source_prefix} only, and returns the empty string for a {@code source_url}
     * configuration: a single named object has no prefix to match blobs against, and the only caller asks for a prefix
     * exactly when the split strategy is prefix-based, which the validator already requires {@code source_prefix} for.
     *
     * @param config the parsed source configuration; must not be {@code null}
     * @return the leaf prefix, the empty string if the prefix ends with {@code /}, or the empty string if
     *         {@code source_url} is configured instead
     */
    public static String prefix(final ObjectStoreSourceConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        return leaf(config.getSourcePrefix());
    }

    private static String configuredPath(final ObjectStoreSourceConfig config) {
        return config.getSourceUrl() != null ? config.getSourceUrl() : config.getSourcePrefix();
    }

    /**
     * Returns the repository-relative directory prefix of the configured {@code source_url}/{@code source_prefix} (the
     * portion up to and including the final {@code /}), or the empty string if the path has no directory portion. This
     * is the prefix that a repository-relative manifest key shares with the configured path; stripping it yields the
     * container-relative object name that {@link #resolveContainer(RepositoriesService, ObjectStoreSourceConfig)}
     * expects.
     *
     * @param config the parsed source configuration; must not be {@code null}
     * @return the directory prefix ending in {@code /}, or the empty string
     */
    public static String directoryPrefix(final ObjectStoreSourceConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        final List<String> segments = directorySegments(configuredPath(config));
        return segments.isEmpty() ? "" : String.join(SEPARATOR, segments) + SEPARATOR;
    }

    /**
     * Translates a repository-relative manifest key into the container-relative object name expected by the container
     * returned from {@link #resolveContainer(RepositoriesService, ObjectStoreSourceConfig)}, by stripping the
     * configured directory prefix. Keys that do not begin with the directory prefix are returned unchanged. The prefix
     * is passed in rather than recomputed, so a caller translating a whole manifest derives it once.
     *
     * @param directoryPrefix        the directory prefix from {@link #directoryPrefix(ObjectStoreSourceConfig)}
     * @param repositoryRelativeKey  the manifest key relative to the repository base path; must not be {@code null}
     * @return the container-relative object name
     */
    public static String containerRelativeKey(final String directoryPrefix, final String repositoryRelativeKey) {
        Objects.requireNonNull(repositoryRelativeKey, "repositoryRelativeKey must not be null");
        if (directoryPrefix.isEmpty() == false && repositoryRelativeKey.startsWith(directoryPrefix)) {
            return repositoryRelativeKey.substring(directoryPrefix.length());
        }
        return repositoryRelativeKey;
    }

    private static List<String> directorySegments(final String path) {
        final List<String> segments = new ArrayList<>();
        if (path == null) {
            return segments;
        }
        final int lastSeparator = path.lastIndexOf(SEPARATOR);
        if (lastSeparator < 0) {
            return segments;
        }
        for (final String segment : path.substring(0, lastSeparator).split(SEPARATOR)) {
            if (segment.isEmpty() == false) {
                segments.add(segment);
            }
        }
        return segments;
    }

    private static String leaf(final String path) {
        if (path == null) {
            return "";
        }
        final int lastSeparator = path.lastIndexOf(SEPARATOR);
        if (lastSeparator < 0) {
            return path;
        }
        return path.substring(lastSeparator + 1);
    }
}
