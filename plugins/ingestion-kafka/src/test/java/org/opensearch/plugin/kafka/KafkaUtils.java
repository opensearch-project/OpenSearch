/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

public class KafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    private static final int CREATE_TOPIC_ATTEMPTS = 3;
    private static final long CREATE_TOPIC_ATTEMPT_TIMEOUT_SECONDS = 30;
    private static final long DESCRIBE_TOPIC_WAIT_SECONDS = 60;

    /**
     * Creates kafka topic
     *
     * @param topicName the topic name
     * @param bootstrapServer kafka bootstrap server list
     */
    public static void createTopic(String topicName, String bootstrapServer) {
        createTopic(topicName, 1, bootstrapServer);
    }

    /**
     * Creates a Kafka topic and waits for it to become visible. Blocks on the create
     * future so transient broker failures surface, retries a small number of times on
     * transient errors, and reuses one {@link AdminClient} across create and describe.
     */
    public static void createTopic(String topicName, int numOfPartitions, String bootstrapServers) {
        try (AdminClient adminClient = createAdminClient(bootstrapServers)) {
            NewTopic newTopic = new NewTopic(topicName, numOfPartitions, (short) 1);

            boolean created = false;
            Throwable lastFailure = null;
            for (int attempt = 1; attempt <= CREATE_TOPIC_ATTEMPTS && created == false; attempt++) {
                try {
                    adminClient.createTopics(List.of(newTopic)).all().get(CREATE_TOPIC_ATTEMPT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    created = true;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        LOGGER.warn("Topic {} already existed", topicName);
                        created = true;
                    } else {
                        lastFailure = e;
                        LOGGER.warn(
                            "createTopic attempt {}/{} for {} failed: {}",
                            attempt,
                            CREATE_TOPIC_ATTEMPTS,
                            topicName,
                            e.getMessage()
                        );
                    }
                } catch (TimeoutException e) {
                    lastFailure = e;
                    LOGGER.warn("createTopic attempt {}/{} for {} timed out", attempt, CREATE_TOPIC_ATTEMPTS, topicName);
                }
                if (created == false && attempt < CREATE_TOPIC_ATTEMPTS) {
                    Thread.sleep(500L * attempt);
                }
            }
            if (created == false) {
                throw new RuntimeException(
                    "Failed to create topic " + topicName + " after " + CREATE_TOPIC_ATTEMPTS + " attempts",
                    lastFailure
                );
            }

            await().atMost(DESCRIBE_TOPIC_WAIT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> topicVisible(adminClient, topicName));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while creating topic " + topicName, e);
        }
    }

    private static boolean topicVisible(AdminClient adminClient, String topicName) {
        try {
            return adminClient.describeTopics(List.of(topicName)).topicNameValues().get(topicName).get().name().equals(topicName);
        } catch (Exception e) {
            return false;
        }
    }

    private static AdminClient createAdminClient(String bootstrapServer) {
        return KafkaAdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer, AdminClientConfig.CLIENT_ID_CONFIG, "test")
        );
    }
}
