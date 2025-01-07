/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import java.util.List;

public interface FastPrefixMatchingStructure {
    void insert(String key, String value);

    List<String> search(String key);

    boolean delete(String key);
}
