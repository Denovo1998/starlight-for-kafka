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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class KafkaPayloadProcessorIntegrationTest extends KopProtocolHandlerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setEntryFormat("kafka");
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60000)
    public void testKafkaProducerToPulsarConsumer() throws Exception {
        String topic = "payload-processor-integration";
        admin.topics().createPartitionedTopic(topic, 1);
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topic)
                     .subscriptionName("payload-sub")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .messagePayloadProcessor(new KafkaPayloadProcessor())
                     .subscribe();
             KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties())) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)).get();
            }
            for (int i = 0; i < 3; i++) {
                Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                assertNotNull(message);
                assertEquals(message.getKey(), "key-" + i);
                assertEquals(message.getValue(), "value-" + i);
                consumer.acknowledge(message);
            }
            assertNull(consumer.receive(1, TimeUnit.SECONDS));
        }
    }
}
