/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 *
 * Class specific to paginated queries, which will contain common query params required by a paginated API.
 */
@PublicApi(since = "3.0.0")
public class PageParams implements Writeable {

    public static final String PARAM_SORT = "sort";
    public static final String PARAM_NEXT_TOKEN = "next_token";
    public static final String PARAM_SIZE = "size";
    public static final String PARAM_ASC_SORT_VALUE = "asc";
    public static final String PARAM_DESC_SORT_VALUE = "desc";

    private final String requestedTokenStr;
    private final String sort;
    private final int size;

    public PageParams(StreamInput in) throws IOException {
        this.requestedTokenStr = in.readOptionalString();
        this.sort = in.readOptionalString();
        this.size = in.readInt();
    }

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(requestedTokenStr);
        out.writeOptionalString(sort);
        out.writeInt(size);
    }
}
