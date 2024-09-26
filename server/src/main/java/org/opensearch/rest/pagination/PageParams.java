/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 * Class specific to paginated queries, which will contain common query params required by a paginated API.
 */
@PublicApi(since = "3.0.0")
public class PageParams {

    public static final String PARAM_SORT = "sort";
    public static final String PARAM_NEXT_TOKEN = "next_token";
    public static final String PARAM_SIZE = "size";
    public static final String PARAM_ASC_SORT_VALUE = "asc";
    public static final String PARAM_DESC_SORT_VALUE = "desc";

    private final String requestedTokenStr;
    private final String sort;
    private final int size;

    public PageParams(String requestedToken, String sort, int size) {
        this.requestedTokenStr = requestedToken;
        this.sort = sort;
        this.size = size;
    }

    public String getSort() {
        return sort;
    }

    public String getRequestedToken() {
        return requestedTokenStr;
    }

    public int getSize() {
        return size;
    }

    public void writePageParams(StreamOutput out) throws IOException {
        out.writeString(requestedTokenStr);
        out.writeString(sort);
        out.writeInt(size);
    }

    public static PageParams readPageParams(StreamInput in) throws IOException {
        String requestedToken = in.readString();
        String sort = in.readString();
        int size = in.readInt();
        return new PageParams(requestedToken, sort, size);
    }

}
