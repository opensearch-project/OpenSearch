/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import java.io.IOException;

/**
 * Plugin interface to expose the template maintaining logic.
 */
public interface SystemTemplatesPlugin {

    /**
     * @return repository implementation from which templates are to be fetched.
     */
    TemplateRepository loadRepository() throws IOException;

    /**
     * @param templateInfo Metadata about the template to load
     * @return Implementation of TemplateLoader which determines how to make the template available at runtime.
     */
    TemplateLoader loaderFor(SystemTemplateInfo templateInfo);
}
