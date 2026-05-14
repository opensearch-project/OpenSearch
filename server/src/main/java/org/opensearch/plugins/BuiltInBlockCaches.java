/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Names for built-in block cache backends.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BuiltInBlockCaches {

    /** Name of the Foyer-backed disk block cache. */
    public static final String FOYER = "foyer";

    private BuiltInBlockCaches() {}
}
