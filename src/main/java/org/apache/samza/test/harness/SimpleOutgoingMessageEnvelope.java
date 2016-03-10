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

import org.apache.samza.system.OutgoingMessageEnvelope;

/**
 * A OutgoingDriverMessageEnvelope used to unit test Samza tasks. Since it is created within
 * individual task instances, it need not have a notion of SystemStreams and their partitions.
 * This DriverMessageEnvelope does not handle the (de)serialization of the Objects it holds.
 *
 * @param <K> Instance of the key of the envelope.
 * @param <V> Instance of the value of the envelope.
 */
public class SimpleOutgoingMessageEnvelope<K, V> extends OutgoingMessageEnvelope {
    private final K key;
    private final V message;

    public SimpleOutgoingMessageEnvelope(K key, V message) {
        super(null, key, message);
        this.key = key;
        this.message = message;
    }

    @Override
    public String toString() {
        return "DriverMessageEnvelope [key=" + key + ", message=" + message + "]";
    }
}