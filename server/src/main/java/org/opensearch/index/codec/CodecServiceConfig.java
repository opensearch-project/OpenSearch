/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;

import java.util.List;
import java.util.Objects;

/**
 * The configuration parameters necessary for the {@link CodecService} instance construction.
 *
 * @opensearch.internal
 */
public final class CodecServiceConfig {
    private final IndexSettings indexSettings;
    private final MapperService mapperService;
    private final Logger logger;
    private final List<AdditionalCodecs> additionalCodecs;

    public CodecServiceConfig(
        IndexSettings indexSettings,
        @Nullable MapperService mapperService,
        @Nullable Logger logger,
        List<AdditionalCodecs> additionalCodecs
    ) {
        this.indexSettings = Objects.requireNonNull(indexSettings);
        this.mapperService = mapperService;
        this.logger = logger;
        this.additionalCodecs = Objects.requireNonNull(additionalCodecs);
    }

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    @Nullable
    public MapperService getMapperService() {
        return mapperService;
    }

    @Nullable
    public Logger getLogger() {
        return logger;
    }

    public List<AdditionalCodecs> getAdditionalCodecs() {
        return additionalCodecs;
    }
}
