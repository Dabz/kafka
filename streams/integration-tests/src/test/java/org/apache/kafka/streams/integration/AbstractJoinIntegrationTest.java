/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.TestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
public abstract class AbstractJoinIntegrationTest {
    private final MockTime time = new MockTime();
    private static final Long COMMIT_INTERVAL = 100L;
    static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    static final String OUTPUT_TOPIC = "outputTopic";
    static final long ANY_UNIQUE_KEY = 0L;

    protected final List<Input<String>> input = Arrays.asList(
        new Input<>(INPUT_TOPIC_LEFT, null, 1),
        new Input<>(INPUT_TOPIC_RIGHT, null, 2),
        new Input<>(INPUT_TOPIC_LEFT, "A", 3),
        new Input<>(INPUT_TOPIC_RIGHT, "a", 4),
        new Input<>(INPUT_TOPIC_LEFT, "B", 5),
        new Input<>(INPUT_TOPIC_RIGHT, "b", 6),
        new Input<>(INPUT_TOPIC_LEFT, null, 7),
        new Input<>(INPUT_TOPIC_RIGHT, null, 8),
        new Input<>(INPUT_TOPIC_LEFT, "C", 9),
        new Input<>(INPUT_TOPIC_RIGHT, "c", 10),
        new Input<>(INPUT_TOPIC_RIGHT, null, 11),
        new Input<>(INPUT_TOPIC_LEFT, null, 12),
        new Input<>(INPUT_TOPIC_RIGHT, null, 13),
        new Input<>(INPUT_TOPIC_RIGHT, "d", 7), // out-of-order data with null as latest
        new Input<>(INPUT_TOPIC_LEFT, "D", 6),
        new Input<>(INPUT_TOPIC_LEFT, null, 2),
        new Input<>(INPUT_TOPIC_RIGHT, null, 3),
        new Input<>(INPUT_TOPIC_RIGHT, "e", 14),
        new Input<>(INPUT_TOPIC_LEFT, "E", 15),
        new Input<>(INPUT_TOPIC_LEFT, null, 10), // out-of-order data with non-null as latest
        new Input<>(INPUT_TOPIC_RIGHT, null, 9),
        new Input<>(INPUT_TOPIC_LEFT, "F", 4),
        new Input<>(INPUT_TOPIC_RIGHT, "f", 3)
    );

    // used for stream-stream join tests where out-of-order data does not meaningfully affect
    // the result, and the main `input` list results in too many result records/test noise.
    // also used for table-table multi-join tests, since out-of-order data with table-table
    // joins is already tested in non-multi-join settings.
    protected final List<Input<String>> inputWithoutOutOfOrderData = Arrays.asList(
        new Input<>(INPUT_TOPIC_LEFT, null, 1),
        new Input<>(INPUT_TOPIC_RIGHT, null, 2),
        new Input<>(INPUT_TOPIC_LEFT, "A", 3),
        new Input<>(INPUT_TOPIC_RIGHT, "a", 4),
        new Input<>(INPUT_TOPIC_LEFT, "B", 5),
        new Input<>(INPUT_TOPIC_RIGHT, "b", 6),
        new Input<>(INPUT_TOPIC_LEFT, null, 7),
        new Input<>(INPUT_TOPIC_RIGHT, null, 8),
        new Input<>(INPUT_TOPIC_LEFT, "C", 9),
        new Input<>(INPUT_TOPIC_RIGHT, "c", 10),
        new Input<>(INPUT_TOPIC_RIGHT, null, 11),
        new Input<>(INPUT_TOPIC_LEFT, null, 12),
        new Input<>(INPUT_TOPIC_RIGHT, null, 13),
        new Input<>(INPUT_TOPIC_RIGHT, "d", 14),
        new Input<>(INPUT_TOPIC_LEFT, "D", 15),
        new Input<>(INPUT_TOPIC_LEFT, null, "E", 16),
        new Input<>(INPUT_TOPIC_RIGHT, null, "e", 17)
    );

    // used for stream-stream self joins where only one input topic is needed
    private final List<Input<String>> leftInput = Arrays.asList(
        new Input<>(INPUT_TOPIC_LEFT, null, 1),
        new Input<>(INPUT_TOPIC_LEFT, "A", 2),
        new Input<>(INPUT_TOPIC_LEFT, "B", 3),
        new Input<>(INPUT_TOPIC_LEFT, null, 4),
        new Input<>(INPUT_TOPIC_LEFT, "C", 5),
        new Input<>(INPUT_TOPIC_LEFT, null, 6),
        new Input<>(INPUT_TOPIC_LEFT, "D", 7)
    );


    final ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> value1 + "-" + value2;

    Properties setupConfigsAndUtils(final boolean cacheEnabled) {
        return setupConfigsAndUtils(cacheEnabled, true);
    }

    Properties setupConfigsAndUtils(final boolean cacheEnabled, final boolean setSerdes) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (setSerdes) {
            streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
            streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        }
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
        if (!cacheEnabled) {
            streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        }

        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        return streamsConfig;
    }

    void runTestWithDriver(
            final List<Input<String>> input,
            final List<List<TestRecord<Long, String>>> expectedResult,
            final Properties properties,
            final Topology topology) {
        runTestWithDriver(input, expectedResult, null, properties, topology);
    }

    void runTestWithDriver(
            final List<Input<String>> input,
            final List<List<TestRecord<Long, String>>> expectedResult,
            final String storeName,
            final Properties properties,
            final Topology topology) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, properties)) {
            final TestInputTopic<Long, String> right = driver.createInputTopic(INPUT_TOPIC_RIGHT, new LongSerializer(), new StringSerializer());
            final TestInputTopic<Long, String> left = driver.createInputTopic(INPUT_TOPIC_LEFT, new LongSerializer(), new StringSerializer());
            final TestOutputTopic<Long, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new LongDeserializer(), new StringDeserializer());
            final Map<String, TestInputTopic<Long, String>> testInputTopicMap = new HashMap<>();

            testInputTopicMap.put(INPUT_TOPIC_RIGHT, right);
            testInputTopicMap.put(INPUT_TOPIC_LEFT, left);

            TestRecord<Long, String> expectedFinalResult = null;

            final long baseTimestamp = time.milliseconds();
            final Iterator<List<TestRecord<Long, String>>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInputRecord : input) {
                testInputTopicMap.get(singleInputRecord.topic).pipeInput(singleInputRecord.record.key, singleInputRecord.record.value, baseTimestamp + singleInputRecord.timestamp);

                final List<TestRecord<Long, String>> expected = resultIterator.next();
                if (expected != null) {
                    final List<TestRecord<Long, String>> updatedExpected = new LinkedList<>();
                    for (final TestRecord<Long, String> record : expected) {
                        updatedExpected.add(new TestRecord<>(record.key(), record.value(), null, baseTimestamp + record.timestamp()));
                    }

                    final List<TestRecord<Long, String>> output = outputTopic.readRecordsToList();
                    assertThat(output, equalTo(updatedExpected));
                    expectedFinalResult = updatedExpected.get(expected.size() - 1);
                } else {
                    final List<TestRecord<Long, String>> output = outputTopic.readRecordsToList();
                    assertThat(output, equalTo(Collections.emptyList()));
                }
            }

            if (storeName != null) {
                checkQueryableStore(storeName, expectedFinalResult, driver);
            }
        }
    }

    void runTestWithDriver(
            final List<Input<String>> input,
            final TestRecord<Long, String> expectedFinalResult,
            final String storeName,
            final Properties streamsConfig,
            final Topology topology) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<Long, String> right = driver.createInputTopic(INPUT_TOPIC_RIGHT, new LongSerializer(), new StringSerializer());
            final TestInputTopic<Long, String> left = driver.createInputTopic(INPUT_TOPIC_LEFT, new LongSerializer(), new StringSerializer());
            final TestOutputTopic<Long, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new LongDeserializer(), new StringDeserializer());
            final Map<String, TestInputTopic<Long, String>> testInputTopicMap = new HashMap<>();

            testInputTopicMap.put(INPUT_TOPIC_RIGHT, right);
            testInputTopicMap.put(INPUT_TOPIC_LEFT, left);

            final long baseTimestamp = time.milliseconds();

            for (final Input<String> singleInputRecord : input) {
                testInputTopicMap.get(singleInputRecord.topic).pipeInput(singleInputRecord.record.key, singleInputRecord.record.value, baseTimestamp + singleInputRecord.timestamp);
            }

            final TestRecord<Long, String> updatedExpectedFinalResult =
                new TestRecord<>(
                    expectedFinalResult.key(),
                    expectedFinalResult.value(),
                    null,
                    baseTimestamp + expectedFinalResult.timestamp());

            final List<TestRecord<Long, String>> output = outputTopic.readRecordsToList();

            assertThat(output.get(output.size() - 1), equalTo(updatedExpectedFinalResult));

            if (storeName != null) {
                checkQueryableStore(storeName, updatedExpectedFinalResult, driver);
            }
        }
    }

    void runSelfJoinTestWithDriver(
            final List<List<TestRecord<Long, String>>> expectedResult,
            final Properties streamsConfig,
            final Topology topology) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<Long, String> left = driver.createInputTopic(INPUT_TOPIC_LEFT, new LongSerializer(), new StringSerializer());
            final TestOutputTopic<Long, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new LongDeserializer(), new StringDeserializer());

            final long firstTimestamp = time.milliseconds();
            long eventTimestamp = firstTimestamp;
            final Iterator<List<TestRecord<Long, String>>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInputRecord : leftInput) {
                left.pipeInput(singleInputRecord.record.key, singleInputRecord.record.value, ++eventTimestamp);

                final List<TestRecord<Long, String>> expected = resultIterator.next();
                if (expected != null) {
                    final List<TestRecord<Long, String>> updatedExpected = new LinkedList<>();
                    for (final TestRecord<Long, String> record : expected) {
                        updatedExpected.add(new TestRecord<>(record.key(), record.value(), null, firstTimestamp + record.timestamp()));
                    }

                    final List<TestRecord<Long, String>> output = outputTopic.readRecordsToList();
                    assertThat(output, equalTo(updatedExpected));
                }
            }
        }
    }

    private void checkQueryableStore(final String queryableName, final TestRecord<Long, String> expectedFinalResult, final TopologyTestDriver driver) {
        final ReadOnlyKeyValueStore<Long, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore(queryableName);

        try (final KeyValueIterator<Long, ValueAndTimestamp<String>> all = store.all()) {
            final KeyValue<Long, ValueAndTimestamp<String>> onlyEntry = all.next();

            assertThat(onlyEntry.key, is(expectedFinalResult.key()));
            assertThat(onlyEntry.value.value(), is(expectedFinalResult.value()));
            assertThat(onlyEntry.value.timestamp(), is(expectedFinalResult.timestamp()));
            assertThat(all.hasNext(), is(false));
        }
    }

    protected static final class Input<V> {
        String topic;
        KeyValue<Long, V> record;
        long timestamp;

        Input(final String topic, final V value, final long timestamp) {
            this.topic = topic;
            record = KeyValue.pair(ANY_UNIQUE_KEY, value);
            this.timestamp = timestamp;
        }

        Input(final String topic, final Long key, final V value, final long timestamp) {
            this.topic = topic;
            record = KeyValue.pair(key, value);
            this.timestamp = timestamp;
        }
    }
}
