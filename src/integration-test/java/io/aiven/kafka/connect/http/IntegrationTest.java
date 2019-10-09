/*
 * Copyright 2019 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.http;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@Testcontainers
final class IntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);

    private static final String HTTP_PATH = "/send-data-here";
    private static final String AUTHORIZATION = "Bearer some-token";
    private static final String CONTENT_TYPE = "application/json";

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_TOPIC_PARTITIONS = 1;

    private static File pluginsDir;

    @Container
    private final KafkaContainer kafka = new KafkaContainer()
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;

    private ConnectRunner connectRunner;

    private MockServer mockServer;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        final File testDir = Files.createTempDirectory("aiven-kafka-connect-transforms-http-").toFile();
        testDir.deleteOnExit();

        pluginsDir = new File(testDir, "plugins/");
        assert pluginsDir.mkdirs();

        // Unpack the library distribution.
        final File transformDir = new File(pluginsDir, "aiven-kafka-connect-http/");
        assert transformDir.mkdirs();
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();
        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
            distFile.toString(), transformDir.toString());
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        mockServer = new MockServer(HTTP_PATH, AUTHORIZATION, CONTENT_TYPE);
        mockServer.start();

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);

        final Map<String, Object> producerProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            "1"
        );
        producer = new KafkaProducer<>(producerProps);

        final NewTopic testTopic = new NewTopic(TEST_TOPIC, TEST_TOPIC_PARTITIONS, (short) 1);
        adminClient.createTopics(List.of(testTopic)).all().get();

        connectRunner = new ConnectRunner(pluginsDir, kafka.getBootstrapServers());
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();

        mockServer.stop();
    }

    @Test
    @Timeout(10)
    final void testBasicDelivery() throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = Map.of(
            "name", "test-source-connector",
            "connector.class", HttpSinkConnector.class.getName(),
            "topics", TEST_TOPIC,
            "key.converter", "org.apache.kafka.connect.storage.StringConverter",
            "value.converter", "org.apache.kafka.connect.storage.StringConverter",
            "tasks.max", "1",
            "http.url", "http://localhost:" + mockServer.localPort() + HTTP_PATH,
            "http.authorization.type", "static",
            "http.headers.authorization", AUTHORIZATION,
            "http.headers.content.type", CONTENT_TYPE
        );
        connectRunner.createConnector(connectorConfig);

        final List<String> expectedBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + cnt;
                final String value = "value-" + cnt;
                expectedBodies.add(value);

                cnt += 1;

                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> mockServer.recorderBodies().size() >= expectedBodies.size(),
            10000,
            "All requests received by HTTP server"
        );
        assertIterableEquals(expectedBodies, mockServer.recorderBodies());

        log.info("{} HTTP requests were expected, {} were successfully delivered",
            expectedBodies.size(),
            mockServer.recorderBodies().size());
    }

    private Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final String value) {
        final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(
            topicName, partition,
            key == null ? null : key.getBytes(),
            value == null ? null : value.getBytes());
        return producer.send(msg);
    }
}
