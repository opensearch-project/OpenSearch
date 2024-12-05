/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;

/**
 * Encapsulates the information and content about a system template available within a repository.
 */
@ExperimentalApi
public class SystemTemplate {

    private final BytesReference templateContent;

    private final SystemTemplateMetadata templateMetadata;

    private final TemplateRepositoryMetadata repositoryMetadata;

    public SystemTemplate(BytesReference templateContent, SystemTemplateMetadata templateInfo, TemplateRepositoryMetadata repositoryInfo) {
        this.templateContent = templateContent;
        this.templateMetadata = templateInfo;
        this.repositoryMetadata = repositoryInfo;
    }

    public BytesReference templateContent() {
        return templateContent;
    }

    public SystemTemplateMetadata templateMetadata() {
        return templateMetadata;
    }

    public TemplateRepositoryMetadata repositoryMetadata() {
        return repositoryMetadata;
    }
}
