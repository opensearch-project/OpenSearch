/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Repository interface around the templates provided by a store (e.g. code repo, remote file store, etc)
 */
@ExperimentalApi
public interface SystemTemplateRepository extends AutoCloseable {

    /**
     * @return Metadata about the repository
     */
    TemplateRepositoryMetadata metadata();

    /**
     * @return Metadata for all available templates
     */
    Iterable<SystemTemplateMetadata> listTemplates() throws IOException;

    /**
     *
     * @param template metadata about template to be fetched
     * @return The actual template content
     */
    SystemTemplate getTemplate(SystemTemplateMetadata template) throws IOException;
}
