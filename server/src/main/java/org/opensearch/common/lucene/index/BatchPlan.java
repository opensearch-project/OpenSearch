/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

/**
 * A provider's decision to serve one {@link BatchRequest}: what it will read, decided without reading document data.
 * Releasing a plan frees whatever the provider held for it; it is safe to release a plan whether or not it was opened.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BatchPlan extends Releasable {}
