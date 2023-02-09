/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.get.MultiGetResponse.Failure;
import org.opensearch.identity.DefaultObjectMapper;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.IdentityConfigConstants;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;

/**
 * This class loads the identity configuration from the identity configuration system index
 */
public class ConfigurationLoader {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final Client client;
    private final String identityIndex;
    private final ClusterService cs;
    private final Settings settings;

    ConfigurationLoader(final Client client, ThreadPool threadPool, final Settings settings, ClusterService cs) {
        super();
        this.client = client;
        this.settings = settings;
        this.identityIndex = settings.get(
            IdentityConfigConstants.IDENTITY_CONFIG_INDEX_NAME,
            IdentityConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX
        );
        this.cs = cs;
        log.debug("Index is: {}", identityIndex);
    }

    Map<CType, SecurityDynamicConfiguration<?>> load(final CType[] events, long timeout, TimeUnit timeUnit) throws InterruptedException,
        TimeoutException {
        final CountDownLatch latch = new CountDownLatch(events.length);
        final Map<CType, SecurityDynamicConfiguration<?>> rs = new HashMap<>(events.length);
        final boolean isDebugEnabled = log.isDebugEnabled();
        loadAsync(events, new ConfigCallback() {

            @Override
            public void success(SecurityDynamicConfiguration<?> dConf) {
                if (latch.getCount() <= 0) {
                    log.error(
                        "Latch already counted down (for {} of {})  (index={})",
                        dConf.getCType().toLCString(),
                        Arrays.toString(events),
                        identityIndex
                    );
                }

                rs.put(dConf.getCType(), dConf);
                latch.countDown();
                if (isDebugEnabled) {
                    log.debug(
                        "Received config for {} (of {}) with current latch value={}",
                        dConf.getCType().toLCString(),
                        Arrays.toString(events),
                        latch.getCount()
                    );
                }
            }

            @Override
            public void singleFailure(Failure failure) {
                log.error(
                    "Failure {} retrieving configuration for {} (index={})",
                    failure == null ? null : failure.getMessage(),
                    Arrays.toString(events),
                    identityIndex
                );
            }

            @Override
            public void noData(String id) {
                CType cType = CType.fromString(id);

                log.warn("No data for {} while retrieving configuration for {}  (index={})", id, Arrays.toString(events), identityIndex);
            }

            @Override
            public void failure(Throwable t) {
                String errorMsg = String.format(
                    Locale.ROOT,
                    "Exception while retrieving configuration for %s (index=%s)",
                    Arrays.toString(events),
                    identityIndex
                );
                log.error(errorMsg, t);
            }
        });

        if (!latch.await(timeout, timeUnit)) {
            // timeout
            throw new TimeoutException(
                "Timeout after "
                    + timeout
                    + ""
                    + timeUnit
                    + " while retrieving configuration for "
                    + Arrays.toString(events)
                    + "(index="
                    + identityIndex
                    + ")"
            );
        }

        return rs;
    }

    void loadAsync(final CType[] events, final ConfigCallback callback) {
        if (events == null || events.length == 0) {
            log.warn("No config events requested to load");
            return;
        }

        final MultiGetRequest mget = new MultiGetRequest();

        for (int i = 0; i < events.length; i++) {
            final String event = events[i].toLCString();
            mget.add(identityIndex, event);
        }

        mget.refresh(true);
        mget.realtime(true);

        client.multiGet(mget, new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse response) {
                MultiGetItemResponse[] responses = response.getResponses();
                for (int i = 0; i < responses.length; i++) {
                    MultiGetItemResponse singleResponse = responses[i];
                    if (singleResponse != null && !singleResponse.isFailed()) {
                        GetResponse singleGetResponse = singleResponse.getResponse();
                        if (singleGetResponse.isExists() && !singleGetResponse.isSourceEmpty()) {
                            // success
                            try {
                                final SecurityDynamicConfiguration<?> dConf = toConfig(singleGetResponse);
                                if (dConf != null) {
                                    callback.success(dConf.deepClone());
                                } else {
                                    callback.failure(new Exception("Cannot parse settings for " + singleGetResponse.getId()));
                                }
                            } catch (Exception e) {
                                log.error(e.toString());
                                callback.failure(e);
                            }
                        } else {
                            // does not exist or empty source
                            callback.noData(singleGetResponse.getId());
                        }
                    } else {
                        // failure
                        callback.singleFailure(singleResponse == null ? null : singleResponse.getFailure());
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                callback.failure(e);
            }
        });

    }

    /**
     * Converts a JSON response to a CType Java object
     * @param singleGetResponse
     * @return SecurityDynamicConfiguration object which is the map of Java objects
     * @throws Exception
     */
    private SecurityDynamicConfiguration<?> toConfig(GetResponse singleGetResponse) throws Exception {
        String sourceAsString = singleGetResponse.getSourceAsString();
        byte[] sourceAsByteArray = sourceAsString.getBytes(StandardCharsets.UTF_8);
        BytesReference ref = new BytesArray(sourceAsByteArray, 0, sourceAsByteArray.length);
        // TODO Figure out why the line below works intermittently. Switching to lines above.
        // BytesReference ref = singleGetResponse.getSourceAsBytesRef();

        final String id = singleGetResponse.getId();
        final long seqNo = singleGetResponse.getSeqNo();
        final long primaryTerm = singleGetResponse.getPrimaryTerm();

        if (ref == null || ref.length() == 0) {
            log.error("Empty or null byte reference for {}", id);
            return null;
        }

        XContentParser parser = null;

        try {
            parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, ref, XContentType.JSON);
            parser.nextToken();
            parser.nextToken();

            if (!id.equals((parser.currentName()))) {
                log.error("Cannot parse config for type {} because {}!={}", id, id, parser.currentName());
                return null;
            }

            parser.nextToken();

            final String jsonAsString = new String(parser.binaryValue(), StandardCharsets.UTF_8);
            final JsonNode jsonNode = DefaultObjectMapper.readTree(jsonAsString);
            int configVersion = 1;

            if (jsonNode.get("_meta") != null) {
                assert jsonNode.get("_meta").get("type").asText().equals(id);
                configVersion = jsonNode.get("_meta").get("config_version").asInt();
            }

            if (log.isDebugEnabled()) {
                log.debug("Load " + id + " with version " + configVersion);
            }

            return SecurityDynamicConfiguration.fromJson(jsonAsString, CType.fromString(id), configVersion, seqNo, primaryTerm);

        } finally {
            if (parser != null) {
                try {
                    parser.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
