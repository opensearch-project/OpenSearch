/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import java.io.IOException;
import java.util.List;

/**
 * Repository interface around the templates provided by a store (e.g. code repo, remote file store, etc)
 */
public interface TemplateRepository extends AutoCloseable {

    /**
     * @return Metadata about the repository
     */
    TemplateRepositoryInfo info();

    /**
     * @return Metadata for all available templates
     */
    Iterable<SystemTemplateInfo> listTemplates() throws IOException;

    /**
     *
     * @param template
     * @return
     */
    SystemTemplate fetchTemplate(SystemTemplateInfo template) throws IOException;
}
