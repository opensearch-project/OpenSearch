/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.DiffableUtils;

import java.util.Map;

public interface StringKeyDiffProvider<V> {

    DiffableUtils.MapDiff<String, V, Map<String, V>> provideDiff();

}
