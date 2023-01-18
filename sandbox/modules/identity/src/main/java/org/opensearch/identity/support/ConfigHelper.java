/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.identity.DefaultObjectMapper;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.index.engine.VersionConflictEngineException;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;

public class ConfigHelper {

    private static final Logger LOGGER = LogManager.getLogger(ConfigHelper.class);

    public static void uploadFile(Client tc, String filepath, String index, CType cType, int configVersion) throws Exception {
        uploadFile(tc, filepath, index, cType, configVersion, false);
    }

    public static void uploadFile(
        Client tc,
        String filepath,
        String index,
        CType cType,
        int configVersion,
        boolean populateEmptyIfFileMissing
    ) throws Exception {
        final String configType = cType.toLCString();
        LOGGER.info(
            "Will update '"
                + configType
                + "' with "
                + filepath
                + " and populate it with empty doc if file missing and populateEmptyIfFileMissing="
                + populateEmptyIfFileMissing
        );

        if (!populateEmptyIfFileMissing) {
            ConfigHelper.fromYamlFile(filepath, cType, configVersion, 0, 0);
        }

        try (Reader reader = createFileOrStringReader(cType, configVersion, filepath, populateEmptyIfFileMissing)) {

            final IndexRequest indexRequest = new IndexRequest(index).id(configType)
                .opType(OpType.CREATE)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .source(configType, readXContent(reader, XContentType.YAML));
            IndexResponse response = tc.index(indexRequest).actionGet();
            final String res = response.getId();

            if (!configType.equals(res)) {
                throw new Exception(
                    "   FAIL: Configuration for '" + configType + "' failed for unknown reasons. Pls. consult logfile of opensearch"
                );
            }
            LOGGER.info("Doc with id '{}' and version {} is updated in {} index.", configType, configVersion, index);
        } catch (VersionConflictEngineException versionConflictEngineException) {
            LOGGER.info("Index {} already contains doc with id {}, skipping update.", index, configType);
        }
    }

    public static Reader createFileOrStringReader(CType cType, int configVersion, String filepath, boolean populateEmptyIfFileMissing)
        throws Exception {
        Reader reader;
        if (!populateEmptyIfFileMissing || Files.exists(Path.of(filepath))) {
            reader = new FileReader(filepath, StandardCharsets.UTF_8);
        } else {
            reader = new StringReader(createEmptySdcYaml(cType, configVersion));
        }
        return reader;
    }

    public static SecurityDynamicConfiguration<?> createEmptySdc(CType cType, int configVersion) throws Exception {
        SecurityDynamicConfiguration<?> empty = SecurityDynamicConfiguration.empty();
        String string = DefaultObjectMapper.writeValueAsString(empty, false);
        SecurityDynamicConfiguration<?> c = SecurityDynamicConfiguration.fromJson(string, cType, configVersion, -1, -1);
        return c;
    }

    public static String createEmptySdcYaml(CType cType, int configVersion) throws Exception {
        return DefaultObjectMapper.YAML_MAPPER.writeValueAsString(createEmptySdc(cType, configVersion));
    }

    public static BytesReference readXContent(final Reader reader, final XContentType xContentType) throws IOException {
        BytesReference retVal;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(xContentType).createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, reader);
            parser.nextToken();
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.copyCurrentStructure(parser);
            retVal = BytesReference.bytes(builder);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        return retVal;
    }

    public static <T> SecurityDynamicConfiguration<T> fromYamlReader(
        Reader yamlReader,
        CType ctype,
        int version,
        long seqNo,
        long primaryTerm
    ) throws IOException {
        try {
            return SecurityDynamicConfiguration.fromNode(
                DefaultObjectMapper.YAML_MAPPER.readTree(yamlReader),
                ctype,
                version,
                seqNo,
                primaryTerm
            );
        } finally {
            if (yamlReader != null) {
                yamlReader.close();
            }
        }
    }

    public static <T> SecurityDynamicConfiguration<T> fromYamlFile(String filepath, CType ctype, int version, long seqNo, long primaryTerm)
        throws IOException {
        return fromYamlReader(new FileReader(filepath, StandardCharsets.UTF_8), ctype, version, seqNo, primaryTerm);
    }

    public static <T> SecurityDynamicConfiguration<T> fromYamlString(
        String yamlString,
        CType ctype,
        int version,
        long seqNo,
        long primaryTerm
    ) throws IOException {
        return fromYamlReader(new StringReader(yamlString), ctype, version, seqNo, primaryTerm);
    }

}
