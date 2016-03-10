/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.test.harness;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;


import org.apache.samza.system.OutgoingMessageEnvelope;

public class InMemoryMessageCollectorTest {
    private InMemoryMessageCollector messageCollector;

    @Before
    public void setup() {
        messageCollector = new InMemoryMessageCollector();
    }

    @Test
    public void testSetAndRemoveListener() {
        assertFalse(messageCollector.hasListeners());
        MessageCollectorListener listener = new KeyAsserterListener();
        messageCollector.registerListener(listener);
        assertTrue(messageCollector.hasListeners());
        messageCollector.removeListener(listener);
        assertFalse(messageCollector.hasListeners());
    }

    @Test
    public void testSendPopulatesInMemoryCollector() {
        SimpleOutgoingMessageEnvelope envelope = new SimpleOutgoingMessageEnvelope<String, String>("key", "value");
        messageCollector.send(envelope);
        assertEquals(1, messageCollector.getMessages().size());
        assertEquals("key", messageCollector.getMessages().get(0).getKey());
    }

    public class KeyAsserterListener implements MessageCollectorListener {
        public void onMessageAdded(OutgoingMessageEnvelope envelope) {
            assertEquals("key", envelope.getKey());
        }
    }
}