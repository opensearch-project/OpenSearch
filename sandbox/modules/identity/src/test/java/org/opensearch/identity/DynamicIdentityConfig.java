/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.identity.configuration.CType;

public class DynamicIdentityConfig {

    private String identityIndexName = ".identity";
    private String identityInternalUsers = "internal_users.yml";
    private String identityConfigAsYamlString = null;
    private String legacyConfigFolder = "";

    public String getIdentityIndexName() {
        return identityIndexName;
    }

    public DynamicIdentityConfig setIdentityIndexName(String identityIndexName) {
        this.identityIndexName = identityIndexName;
        return this;
    }

    public DynamicIdentityConfig setConfigAsYamlString(String identityConfigAsYamlString) {
        this.identityConfigAsYamlString = identityConfigAsYamlString;
        return this;
    }

    public DynamicIdentityConfig setIdentityInternalUsers(String identityInternalUsers) {
        this.identityInternalUsers = identityInternalUsers;
        return this;
    }

    public List<IndexRequest> getDynamicConfig(String folder) {

        final String prefix = legacyConfigFolder + (folder == null ? "" : folder + "/");

        List<IndexRequest> ret = new ArrayList<IndexRequest>();

        ret.add(
            new IndexRequest(identityIndexName).id(CType.INTERNALUSERS.toLCString())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(CType.INTERNALUSERS.toLCString(), FileHelper.readYamlContent(prefix + identityInternalUsers))
        );

        return Collections.unmodifiableList(ret);
    }

}
