/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Pagination response metadata for a paginated query.
 * @opensearch.internal
 */
public class PageToken implements Writeable {

    public static final String PAGINATED_RESPONSE_NEXT_TOKEN_KEY = "next_token";

    /**
     * String denoting the next_token of paginated response, which will be used to fetch next page (if any).
     */
    private final String nextToken;

    /**
     * String denoting the element which is being paginated (for e.g. shards, indices..).
     */
    private final String paginatedEntity;

    public PageToken(String nextToken, String paginatedElement) {
        assert paginatedElement != null : "paginatedElement must be specified for a paginated response";
        this.nextToken = nextToken;
        this.paginatedEntity = paginatedElement;
    }

    public PageToken(StreamInput in) throws IOException {
        this.nextToken = in.readOptionalString();
        this.paginatedEntity = in.readString();
    }

    public String getNextToken() {
        return nextToken;
    }

    public String getPaginatedEntity() {
        return paginatedEntity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nextToken);
        out.writeString(paginatedEntity);
    }

    // Overriding equals and hashcode for testing
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageToken that = (PageToken) o;
        return Objects.equals(this.nextToken, that.nextToken) && Objects.equals(this.paginatedEntity, that.paginatedEntity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextToken, paginatedEntity);
    }
}
