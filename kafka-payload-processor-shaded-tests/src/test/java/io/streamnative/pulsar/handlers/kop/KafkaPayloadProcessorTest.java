/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KafkaPayloadProcessorTest {

    @Test
    public void testRelocatedArtifact() throws Exception {
        assertEquals(KafkaPayloadProcessor.class.getDeclaredField("LOCAL_SINGLE_MESSAGE_METADATA").getType().getName(),
                "org.apache.pulsar.shade.io.netty.util.concurrent.FastThreadLocal");
    }

    @DataProvider(name = "compressionTypes")
    public Object[][] compressionTypes() {
        return Arrays.stream(CompressionType.values()).map(type -> new Object[]{type}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "compressionTypes")
    public void testValues(CompressionType compressionType) throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(Compression.of(compressionType).build(),
                new SimpleRecord("hello".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(new byte[0]), new SimpleRecord((byte[]) null));
        assertValues(records.buffer(), Arrays.asList("hello", "", null));
    }

    @Test
    public void testMultipleBatchesAndControlRecords() throws Exception {
        MemoryRecords first = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("first".getBytes()));
        MemoryRecords control = MemoryRecords.withEndTransactionMarker(0, (short) 0,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));
        MemoryRecords second = MemoryRecords.withRecords(Compression.gzip().build(),
                new SimpleRecord("second".getBytes()), new SimpleRecord("third".getBytes()));
        ByteBuffer buffer = ByteBuffer.allocate(first.sizeInBytes() + control.sizeInBytes() + second.sizeInBytes());
        buffer.put(first.buffer()).put(control.buffer()).put(second.buffer()).flip();
        assertValues(buffer, Arrays.asList("first", "second", "third"));
    }

    @Test
    public void testControlRecordOnly() throws Exception {
        assertValues(MemoryRecords.withEndTransactionMarker(0, (short) 0,
                new EndTransactionMarker(ControlRecordType.ABORT, 0)).buffer(), new ArrayList<>());
    }

    @Test
    public void testKeyAndHeaders() throws Exception {
        byte[] key = "my-key".getBytes(StandardCharsets.UTF_8);
        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(0, key, "value".getBytes(StandardCharsets.UTF_8),
                        new RecordHeader[]{new RecordHeader("header", "content".getBytes(StandardCharsets.UTF_8))}));
        new KafkaPayloadProcessor().process(new ByteBufferPayload(records.buffer()),
                new MockedMessagePayloadContext(1, new MessageIdImpl(0, 0, -1)), Schema.STRING, message -> {
                    assertEquals(message.getKey(), "my-key");
                    assertEquals(message.getOrderingKey(), key);
                    assertEquals(message.getProperty("header"), "content");
                    message.release();
                });
    }

    @Test
    public void testPulsarFormatUsesDefaultProcessor() throws Exception {
        final List<String> values = new ArrayList<>();
        MockedMessagePayloadContext context = new MockedMessagePayloadContext(1, new MessageIdImpl(0, 0, -1)) {
            @Override
            public String getProperty(String key) {
                return null;
            }

            @Override
            public boolean isBatch() {
                return false;
            }
        };
        new KafkaPayloadProcessor().process(new ByteBufferPayload(ByteBuffer.wrap("pulsar".getBytes())),
                context, Schema.STRING, message -> {
                    values.add(message.getValue());
                    message.release();
                });
        assertEquals(values, Arrays.asList("pulsar"));
    }

    private void assertValues(ByteBuffer buffer, List<String> expected) throws Exception {
        List<String> values = new ArrayList<>();
        new KafkaPayloadProcessor().process(new ByteBufferPayload(buffer),
                new MockedMessagePayloadContext(expected.size(), new MessageIdImpl(0, 0, -1)),
                Schema.STRING, message -> {
                    assertEquals(((BatchMessageIdImpl) message.getMessageId()).getBatchIndex(), values.size());
                    values.add(message.getValue());
                    message.release();
                });
        assertEquals(values, expected);
    }
}
