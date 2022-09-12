/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

/**
 * Principal for extension request. (will be replaced by shiro principal)
 */
public class Principal {
    private String name;

    public Principal(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
