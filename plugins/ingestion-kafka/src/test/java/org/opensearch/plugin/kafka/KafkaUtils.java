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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;

public class KafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);

    /**
     * Creates kafka topic
     *
     * @param topicName the topic name
     * @param bootstrapServer kafka bootstrap server list
     */
    public static void createTopic(String topicName, String bootstrapServer) {
        createTopic(topicName, 1, bootstrapServer);
    }

    public static void createTopic(String topicName, int numOfPartitions, String bootstrapServers) {
        try {
            getAdminClient(bootstrapServers, (client -> {
                NewTopic newTopic = new NewTopic(topicName, numOfPartitions, (short) 1);
                client.createTopics(List.of(newTopic));
                return true;
            }));

        } catch (TopicExistsException e) {
            // Catch TopicExistsException otherwise it will break maven-surefire-plugin
            LOGGER.warn("Topic {} already existed", topicName);
        }

        // validates topic is created
        await().atMost(3, TimeUnit.SECONDS).until(() -> checkTopicExistence(topicName, bootstrapServers));
    }

    public static boolean checkTopicExistence(String topicName, String bootstrapServers) {
        return getAdminClient(bootstrapServers, (client -> {
            Map<String, KafkaFuture<TopicDescription>> topics = client.describeTopics(List.of(topicName)).values();

            try {
                return topics.containsKey(topicName) && topics.get(topicName).get().name().equals(topicName);
            } catch (InterruptedException e) {
                LOGGER.error("error on checkTopicExistence", e);
                return false;
            } catch (ExecutionException e) {
                LOGGER.error("error on checkTopicExistence", e);
                return false;
            }
        }));
    }

    private static <Rep> Rep getAdminClient(String bootstrapServer, Function<AdminClient, Rep> function) {
        AdminClient adminClient = KafkaAdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer, AdminClientConfig.CLIENT_ID_CONFIG, "test")
        );
        try {
            return function.apply(adminClient);
        } finally {
            adminClient.close();
        }
    }
}
