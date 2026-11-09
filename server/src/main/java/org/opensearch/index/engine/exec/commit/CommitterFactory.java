/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Committer factory which engine plugins will vend to enable plugging committers in.
 * This wil wire into DataFormarAwareEngine to allow commits to be data-format agnostic.
 */
@ExperimentalApi
@FunctionalInterface
public interface CommitterFactory {
    Committer getCommitter(CommitterConfig committerConfig) throws IOException;
}
