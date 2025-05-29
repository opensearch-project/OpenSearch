/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Profile shard level result that corresponds to a {@link AbstractProfileResult}
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public abstract class AbstractProfileShardResult<T extends AbstractProfileResult<T>> implements Writeable, ToXContentObject {

    protected final List<T> profileResults;

    public AbstractProfileShardResult(List<T> profileResults) {
        this.profileResults = profileResults;
    }

    public AbstractProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        profileResults = new ArrayList<>(profileSize);
        for (int j = 0; j < profileSize; j++) {
            profileResults.add(createProfileResult(in));
        }
    }

    public List<T> getProfileResults() {
        return Collections.unmodifiableList(profileResults);
    }

    public abstract T createProfileResult(StreamInput in) throws IOException;

}
