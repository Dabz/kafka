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
package org.apache.kafka.streams.processor.internals;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.utils.Utils.getNullableSizePrefixedArray;
import static org.apache.kafka.common.utils.Utils.getOptionalField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ProcessorRecordContextTest {
    // timestamp + offset + partition: 8 + 8 + 4
    private static final long MIN_SIZE = 20L;
    private static final String SOURCE_RAW_KEY = "sourceRawKey";
    private static final byte[] SOURCE_RAW_KEY_BYTES = SOURCE_RAW_KEY.getBytes();
    private static final String SOURCE_RAW_VALUE = "sourceRawValue";
    private static final byte[] SOURCE_RAW_VALUE_BYTES = SOURCE_RAW_VALUE.getBytes();

    @Test
    public void shouldNotAllowNullHeaders() {
        assertThrows(
            NullPointerException.class,
            () -> new ProcessorRecordContext(
                42L,
                73L,
                0,
                "topic",
                null
            )
        );
    }

    @Test
    public void shouldEstimateNullTopicAndEmptyHeadersAsZeroLength() {
        final Headers headers = new RecordHeaders();
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateEmptyHeaderAsZeroLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateTopicLength() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders()
        );

        assertEquals(MIN_SIZE + 5L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateHeadersLength() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", "header-value".getBytes());
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L + 12L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldEstimateNullValueInHeaderAsZero() {
        final Headers headers = new RecordHeaders();
        headers.add("header-key", null);
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            null,
            headers
        );

        assertEquals(MIN_SIZE + 10L, context.residentMemorySizeEstimate());
    }

    @Test
    public void shouldSerializeProcessorRecordContext() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders()
        );

        final ByteBuffer serializedContext = ByteBuffer.wrap(context.serialize());

        assertEquals(42L, serializedContext.getLong());
        assertEquals(73L, serializedContext.getLong());
        assertEquals("topic", new String(getNullableSizePrefixedArray(serializedContext), UTF_8));
        assertEquals(0, serializedContext.getInt());
        assertEquals(0, serializedContext.getInt());
        assertFalse(serializedContext.hasRemaining());
    }

    @Test
    public void shouldSerializeProcessorRecordContextWithRawKey() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders(),
            SOURCE_RAW_KEY_BYTES,
            null
        );

        final ByteBuffer serializedContext = ByteBuffer.wrap(context.serialize());

        assertEquals(42L, serializedContext.getLong());
        assertEquals(73L, serializedContext.getLong());
        assertEquals("topic", new String(getNullableSizePrefixedArray(serializedContext), UTF_8));
        assertEquals(0, serializedContext.getInt());
        assertEquals(0, serializedContext.getInt());

        final Optional<byte[]> rawKey = getOptionalField('k', serializedContext);
        assertTrue(rawKey.isPresent());
        assertEquals(SOURCE_RAW_KEY, new String(rawKey.get(), UTF_8));

        assertFalse(serializedContext.hasRemaining());
    }

    @Test
    public void shouldSerializeProcessorRecordContextWithRawValue() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders(),
            null,
            SOURCE_RAW_VALUE_BYTES
        );

        final ByteBuffer serializedContext = ByteBuffer.wrap(context.serialize());

        assertEquals(42L, serializedContext.getLong());
        assertEquals(73L, serializedContext.getLong());
        assertEquals("topic", new String(getNullableSizePrefixedArray(serializedContext), UTF_8));
        assertEquals(0, serializedContext.getInt());
        assertEquals(0, serializedContext.getInt());

        final Optional<byte[]> rawValue = getOptionalField('v', serializedContext);
        assertTrue(rawValue.isPresent());
        assertEquals(SOURCE_RAW_VALUE, new String(rawValue.get(), UTF_8));

        assertFalse(serializedContext.hasRemaining());
    }

    @Test
    public void shouldSerializeProcessorRecordContextWithBothRawKeyAndRawValue() {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders(),
            SOURCE_RAW_KEY_BYTES,
            SOURCE_RAW_VALUE_BYTES
        );

        final ByteBuffer serializedContext = ByteBuffer.wrap(context.serialize());

        assertEquals(42L, serializedContext.getLong());
        assertEquals(73L, serializedContext.getLong());
        assertEquals("topic", new String(getNullableSizePrefixedArray(serializedContext), UTF_8));
        assertEquals(0, serializedContext.getInt());
        assertEquals(0, serializedContext.getInt());

        final Optional<byte[]> rawKey = getOptionalField('k', serializedContext);
        assertTrue(rawKey.isPresent());
        assertEquals(SOURCE_RAW_KEY, new String(rawKey.get(), UTF_8));

        final Optional<byte[]> rawValue = getOptionalField('v', serializedContext);
        assertTrue(rawValue.isPresent());
        assertEquals(SOURCE_RAW_VALUE, new String(rawValue.get(), UTF_8));

        assertFalse(serializedContext.hasRemaining());
    }

    @ParameterizedTest
    @CsvSource(value = {
        "null,null",
        "rawKey,null",
        "null,rawValue",
        "rawKey,rawValue"
    }, nullValues = "null")
    public void shouldDeserializeProcessorRecordContext(final String rawKey, final String rawValue) {
        final ProcessorRecordContext context = new ProcessorRecordContext(
            42L,
            73L,
            0,
            "topic",
            new RecordHeaders(),
            rawKey != null ? rawKey.getBytes() : null,
            rawValue != null ? rawValue.getBytes() : null
        );

        final byte[] serializedContext = context.serialize();
        final ProcessorRecordContext deserializedContext = ProcessorRecordContext.deserialize(ByteBuffer.wrap(serializedContext));

        assertEquals(context, deserializedContext);
    }
}
