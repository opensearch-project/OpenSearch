/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;

/**
 * Configuration required to create an indexing engine.
 */
@ExperimentalApi
public record IndexingConfiguration(MapperService mapperService,
                                    IndexSettings indexSettings
                                    // any more inputs needed can be added here ...
                                    ) {

}
