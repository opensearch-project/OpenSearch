/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import org.opensearch.cluster.applicationtemplates.*;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestSystemTemplatesRepositoryPlugin extends Plugin implements SystemTemplatesPlugin {

    private SystemTemplateMetadata info = SystemTemplateMetadata.createComponentTemplateInfo("dummy", 1);

    private TemplateRepositoryMetadata repoInfo = new TemplateRepositoryMetadata("test", 1);

    private SystemTemplate systemTemplate = new SystemTemplate(BytesReference.fromByteBuffer(ByteBuffer.wrap("content".getBytes(StandardCharsets.UTF_8))), info, repoInfo);

    @Override
    public SystemTemplateRepository loadRepository() throws IOException {
        return new SystemTemplateRepository() {
            @Override
            public TemplateRepositoryMetadata metadata() {
                return repoInfo;
            }

            @Override
            public List<SystemTemplateMetadata> listTemplates() throws IOException {
                return List.of(info);
            }

            @Override
            public SystemTemplate getTemplate(SystemTemplateMetadata template) throws IOException {
                return systemTemplate;
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    @Override
    public SystemTemplateLoader loaderFor(SystemTemplateMetadata templateInfo) {
        return new SystemTemplateLoader() { // Asserting Loader
            @Override
            public void loadTemplate(SystemTemplate template) throws IOException {
                assert template.templateInfo() == info;
                assert template.repositoryInfo() == repoInfo;
                assert template.templateContent() == systemTemplate;
            }
        };
    }
}
