/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import org.opensearch.core.common.bytes.BytesReference;

/**
 * Encapsulates the information and content about a system template available within a repository.
 */
public class SystemTemplate {

    private final BytesReference templateContent;

    private final SystemTemplateInfo templateInfo;

    private final TemplateRepositoryInfo repositoryInfo;

    public SystemTemplate(BytesReference templateContent, SystemTemplateInfo templateInfo, TemplateRepositoryInfo repositoryInfo) {
        this.templateContent = templateContent;
        this.templateInfo = templateInfo;
        this.repositoryInfo = repositoryInfo;
    }

    public BytesReference templateContent() {
        return templateContent;
    }

    public SystemTemplateInfo templateInfo() {
        return templateInfo;
    }

    public TemplateRepositoryInfo repositoryInfo() {
        return repositoryInfo;
    }
}
