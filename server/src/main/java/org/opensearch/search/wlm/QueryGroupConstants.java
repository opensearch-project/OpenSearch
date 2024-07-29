/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.wlm;

import java.util.function.Supplier;

/**
 * This class will hold all the QueryGroup related constants
 */
public class QueryGroupConstants {
    public static final String QUERY_GROUP_ID_HEADER = "queryGroupId";
    public static final Supplier<String> DEFAULT_QUERY_GROUP_ID_SUPPLIER = () -> "DEFAULT_QUERY_GROUP";
}
