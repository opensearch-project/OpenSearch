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
 * Interface to load template into the OpenSearch runtime.
 */
@ExperimentalApi
public interface SystemTemplateLoader {

    /**
     * @param template Templated to be loaded
     * @throws IOException If an exceptional situation is encountered while parsing/loading the template
     */
    boolean loadTemplate(SystemTemplate template) throws IOException;
}
