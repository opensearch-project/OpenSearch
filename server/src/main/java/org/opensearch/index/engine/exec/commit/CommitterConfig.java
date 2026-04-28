/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineConfig;

/**
 * Initialization parameters for a {@link Committer}.
 * Carries the engine configuration needed to set up the backing store.
 *
 * @param engineConfig the engine configuration (nullable — may be absent in tests or standalone mode)
 * @opensearch.experimental
 */
@ExperimentalApi
public record CommitterConfig(EngineConfig engineConfig) {
}
