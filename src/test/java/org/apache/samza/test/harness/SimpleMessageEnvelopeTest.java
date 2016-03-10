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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SimpleMessageEnvelopeTest {
    private SimpleIncomingMessageEnvelope inEnvelope;
    private SimpleOutgoingMessageEnvelope outEnvelope;

    @Before
    public void setup() {
        String key = "key";
        String value = "value";
        inEnvelope = new SimpleIncomingMessageEnvelope<String, String>(key, value);
        outEnvelope = new SimpleOutgoingMessageEnvelope<String, String>(key, value);
    }

    @Test
    public void testKeyValue() {
        assertEquals("key", inEnvelope.getKey());
        assertEquals("value", inEnvelope.getMessage());
        assertEquals("key", outEnvelope.getKey());
        assertEquals("value", outEnvelope.getMessage());
    }

    @Test
    public void testEqual() {
        assertFalse(outEnvelope.equals(inEnvelope));
    }
}
