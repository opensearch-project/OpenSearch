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

package org.opensearch.repositories.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.nio.file.Path;
import java.util.function.Function;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code location}</dt><dd>Path to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size.
 *      Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 *
 * @opensearch.internal
 */
public class FsRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(FsRepository.class);

    public static final String TYPE = "fs";

    public static final Setting<String> LOCATION_SETTING = new Setting<>("location", "", Function.identity(), Property.NodeScope);
    public static final Setting<String> REPOSITORIES_LOCATION_SETTING = new Setting<>(
        "repositories.fs.location",
        LOCATION_SETTING,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "chunk_size",
        new ByteSizeValue(Long.MAX_VALUE),
        new ByteSizeValue(5),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> REPOSITORIES_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "repositories.fs.chunk_size",
        new ByteSizeValue(Long.MAX_VALUE),
        new ByteSizeValue(5),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );
    public static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false, Property.NodeScope);
    public static final Setting<Boolean> REPOSITORIES_COMPRESS_SETTING = Setting.boolSetting(
        "repositories.fs.compress",
        false,
        Property.NodeScope,
        Property.Deprecated
    );

    public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    private final Environment environment;

    private ByteSizeValue chunkSize;

    private final BlobPath basePath;

    /**
     * Constructs a shared file system repository.
     */
    public FsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, calculateCompress(metadata, environment), namedXContentRegistry, clusterService, recoverySettings);
        this.environment = environment;
        String location = REPOSITORIES_LOCATION_SETTING.get(metadata.settings());
        if (location.isEmpty()) {
            logger.warn(
                "the repository location is missing, it should point to a shared file system location"
                    + " that is available on all cluster-manager and data nodes"
            );
            throw new RepositoryException(metadata.name(), "missing location");
        }
        Path locationFile = environment.resolveRepoFile(location);
        if (locationFile == null) {
            if (environment.repoFiles().length > 0) {
                logger.warn(
                    "The specified location [{}] doesn't start with any " + "repository paths specified by the path.repo setting: [{}] ",
                    location,
                    environment.repoFiles()
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo"
                );
            } else {
                logger.warn(
                    "The specified location [{}] should start with a repository path specified by"
                        + " the path.repo setting, but the path.repo setting was not set on this node",
                    location
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo because this setting is empty"
                );
            }
        }

        if (CHUNK_SIZE_SETTING.exists(metadata.settings())) {
            this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        } else {
            this.chunkSize = REPOSITORIES_CHUNK_SIZE_SETTING.get(environment.settings());
        }
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
    }

    private static boolean calculateCompress(RepositoryMetadata metadata, Environment environment) {
        return COMPRESS_SETTING.exists(metadata.settings())
            ? COMPRESS_SETTING.get(metadata.settings())
            : REPOSITORIES_COMPRESS_SETTING.get(environment.settings());
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
        final Path locationFile = environment.resolveRepoFile(location);
        return new FsBlobStore(bufferSize, locationFile, isReadOnly());
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }
}
