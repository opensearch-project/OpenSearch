/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;

import java.util.List;

public class OpenSearchPermission implements Permission {

    String permissionString;

    @Override
    public boolean implies(Permission p) {
        // pseudo-code

        // Example
        // OpenSearch permission instance = OpenSearchPermission("indices:data/read/search|my-index*,other-index*")

        // caller is asking does "indices:data/read/search|my-index*,other-index*"
        // imply "indices:data/read/search|my-index1"
        // YES

        // caller is asking does "indices:admin/create|my-index1"
        // imply "indices:data/read/search|my-index1"
        // NO

        // caller is asking does "indices:data/read/*|my-index1"
        // imply "indices:data/read/search|my-index1"
        // YES

        // OpenSearch Permission: indices:data:read:*
        // Requested Permission: indices:data:read:search

        // WildcardPermission wp = new WildcardPermission(actionNameFromPermission.replace("/", ':'));
        // if (!wp.implies(new WildcardPermission(requestedActionName.place("/", ":")))) {
        //     return false;
        // }

        // Next question: my_index1 in my_index*,other_index*
        // Resolve my_index*,other_index* to concrete indices
        // Then is my_index1 in resolved indices

        // Cluster has my_index1, my_index2, other_index1

        // resolveIndices("my-index*,other-index*) -> List.of("my-index1", "my-index2", "my-index3")

        // String actionName = extractActionName(p.toString());
        // List<String> indexPatterns = extractIndexPatterns(p.toString());
        // List<String> resolvedIndices = resolveIndexPatterns(indexPatterns);
        return false;
    }
}
