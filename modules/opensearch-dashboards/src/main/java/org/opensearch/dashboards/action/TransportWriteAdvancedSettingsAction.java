/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransportWriteAdvancedSettingsAction extends HandledTransportAction<WriteAdvancedSettingsRequest, AdvancedSettingsResponse> {

    private static final Logger log = LogManager.getLogger(TransportWriteAdvancedSettingsAction.class);
    private static final DateFormatter STRICT_DATE_TIME = DateFormatter.forPattern("strict_date_time");
    private static final List<Integer> VALID_OSD_MAJOR_VERSIONS = List.of(1, 2, 3);
    private static final Pattern RC_VERSION_PATTERN = Pattern.compile("^(\\d+\\.\\d+\\.\\d+)-rc(\\d+)$", Pattern.CASE_INSENSITIVE);

    private final Client client;
    private final String configKey;
    private final int buildNum;

    @Inject
    public TransportWriteAdvancedSettingsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(WriteAdvancedSettingsAction.NAME, transportService, actionFilters, WriteAdvancedSettingsRequest::new);
        this.client = client;
        this.configKey = "config:" + Version.CURRENT.toString();
        this.buildNum = Version.CURRENT.id;
    }

    @Override
    protected void doExecute(Task task, WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        if (request.isCreateOperation()) {
            handleCreateOperation(request, listener);
        } else {
            handleUpdateOperation(request, listener);
        }
    }

    private void handleCreateOperation(WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        findUpgradeableConfigSource(request.getIndex(), ActionListener.wrap(upgradeableConfig -> {
            Map<String, Object> doc = buildCreateDocument(request.getSettings(), upgradeableConfig, currentTimestamp(), buildNum);
            IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(configKey).create(true).source(doc);
            client.index(
                indexRequest,
                ActionListener.wrap(indexResponse -> listener.onResponse(new AdvancedSettingsResponse(doc)), listener::onFailure)
            );
        }, listener::onFailure));
    }

    private void handleUpdateOperation(WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        getConfigDocumentSource(request.getIndex(), ActionListener.wrap(existingDoc -> {
            if (existingDoc == null) {
                handleCreateOperation(request, listener);
                return;
            }

            Map<String, Object> doc = buildUpdatedDocument(existingDoc, request.getSettings(), currentTimestamp());
            IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(configKey).source(doc);
            client.index(
                indexRequest,
                ActionListener.wrap(indexResponse -> listener.onResponse(new AdvancedSettingsResponse(doc)), listener::onFailure)
            );
        }, listener::onFailure));
    }

    private void getConfigDocumentSource(String index, ActionListener<Map<String, Object>> listener) {
        client.get(new GetRequest(index, configKey), ActionListener.wrap(response -> {
            if (response.isExists()) {
                listener.onResponse(response.getSourceAsMap());
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private void findUpgradeableConfigSource(String index, ActionListener<Map<String, Object>> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("type", "config"))
            .sort("config.buildNum", SortOrder.DESC)
            .size(1000);

        client.search(new SearchRequest(index).source(sourceBuilder), ActionListener.wrap(response -> {
            String currentVersion = Version.CURRENT.toString();
            for (SearchHit hit : response.getHits().getHits()) {
                String savedVersion = extractConfigVersion(hit.getId());
                if (savedVersion != null && isConfigVersionUpgradeable(savedVersion, currentVersion)) {
                    listener.onResponse(hit.getSourceAsMap());
                    return;
                }
            }
            listener.onResponse(null);
        }, listener::onFailure));
    }

    private String currentTimestamp() {
        return STRICT_DATE_TIME.format(Instant.now());
    }

    static Map<String, Object> buildCreateDocument(
        Map<String, Object> newSettings,
        Map<String, Object> upgradeableConfig,
        String updatedAt,
        int buildNum
    ) {
        Map<String, Object> attributes = new HashMap<>();

        Object priorConfig = upgradeableConfig == null ? null : upgradeableConfig.get("config");
        if (priorConfig instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> priorConfigMap = (Map<String, Object>) priorConfig;
            attributes.putAll(priorConfigMap);
        }

        attributes.put("buildNum", buildNum);
        attributes.putAll(newSettings);

        Map<String, Object> doc = new HashMap<>();
        doc.put("type", "config");
        doc.put("config", attributes);
        doc.put("references", List.of());
        doc.put("updated_at", updatedAt);
        return doc;
    }

    static Map<String, Object> buildUpdatedDocument(Map<String, Object> existingDoc, Map<String, Object> newSettings, String updatedAt) {
        Map<String, Object> doc = new HashMap<>(existingDoc);
        Map<String, Object> attributes = new HashMap<>();

        Object existingConfig = existingDoc.get("config");
        if (existingConfig instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> existingConfigMap = (Map<String, Object>) existingConfig;
            attributes.putAll(existingConfigMap);
        }

        attributes.putAll(newSettings);

        doc.put("type", "config");
        doc.put("config", attributes);
        doc.putIfAbsent("references", List.of());
        doc.put("updated_at", updatedAt);
        return doc;
    }

    static String extractConfigVersion(String rawId) {
        if (rawId == null || rawId.startsWith("config:") == false) {
            return null;
        }
        return rawId.substring("config:".length());
    }

    static boolean isConfigVersionUpgradeable(String savedVersion, String currentVersion) {
        if (savedVersion == null
            || currentVersion == null
            || savedVersion.equals(currentVersion)
            || savedVersion.matches("(?i).*(alpha|beta|snapshot).*")) {
            return false;
        }

        VersionParts saved = VersionParts.parse(savedVersion);
        VersionParts current = VersionParts.parse(currentVersion);
        if (saved == null || current == null) {
            return false;
        }

        boolean savedIsLessThanCurrent = saved.releaseVersion.compareTo(current.releaseVersion) < 0;
        boolean savedIsSameAsCurrent = saved.releaseVersion.equals(current.releaseVersion);
        boolean savedRcIsLessThanCurrent = saved.rcNumber < current.rcNumber;
        boolean savedIsFromPrefork = saved.releaseVersion.compareTo(ReleaseVersion.parse("6.8.0")) >= 0
            && saved.releaseVersion.compareTo(ReleaseVersion.parse("7.10.2")) <= 0;
        boolean currentVersionIsValidOsdVersion = VALID_OSD_MAJOR_VERSIONS.contains(current.releaseVersion.major);

        return savedIsLessThanCurrent
            || (savedIsSameAsCurrent && savedRcIsLessThanCurrent)
            || (savedIsFromPrefork && currentVersionIsValidOsdVersion);
    }

    private static class VersionParts {
        private final ReleaseVersion releaseVersion;
        private final int rcNumber;

        private VersionParts(ReleaseVersion releaseVersion, int rcNumber) {
            this.releaseVersion = releaseVersion;
            this.rcNumber = rcNumber;
        }

        private static VersionParts parse(String version) {
            Matcher rcMatcher = RC_VERSION_PATTERN.matcher(version);
            if (rcMatcher.matches()) {
                return new VersionParts(ReleaseVersion.parse(rcMatcher.group(1)), Integer.parseInt(rcMatcher.group(2)));
            }
            return new VersionParts(ReleaseVersion.parse(version), Integer.MAX_VALUE);
        }
    }

    private static class ReleaseVersion implements Comparable<ReleaseVersion> {
        private final int major;
        private final int minor;
        private final int patch;

        private ReleaseVersion(int major, int minor, int patch) {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
        }

        private static ReleaseVersion parse(String version) {
            String[] parts = version.split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Unsupported release version [" + version + "]");
            }
            return new ReleaseVersion(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
        }

        @Override
        public int compareTo(ReleaseVersion other) {
            if (major != other.major) {
                return Integer.compare(major, other.major);
            }
            if (minor != other.minor) {
                return Integer.compare(minor, other.minor);
            }
            return Integer.compare(patch, other.patch);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj instanceof ReleaseVersion) == false) {
                return false;
            }
            ReleaseVersion other = (ReleaseVersion) obj;
            return major == other.major && minor == other.minor && patch == other.patch;
        }

        @Override
        public int hashCode() {
            return major * 10_000 + minor * 100 + patch;
        }
    }
}
