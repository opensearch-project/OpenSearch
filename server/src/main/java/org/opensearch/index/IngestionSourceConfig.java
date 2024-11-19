/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentParser;

@ExperimentalApi
public interface IngestionSourceConfig extends ToXContentFragment {
    String getIngestionSourceType();

    static IngestionSourceConfig fromXContent(XContentParser parser, String ingestionSourceType) {
        if(ingestionSourceType.equals("kafka")) {
            return KafkaSourceConfig.fromXContent(parser);
        }

        return null;

    }
}
