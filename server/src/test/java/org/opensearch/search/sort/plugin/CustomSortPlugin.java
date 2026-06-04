/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort.plugin;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.List;

public class CustomSortPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SortSpec<?>> getSorts() {
        return Collections.singletonList(new SortSpec<>(CustomSortBuilder.NAME, CustomSortBuilder::new, CustomSortBuilder::fromXContent));
    }
}
