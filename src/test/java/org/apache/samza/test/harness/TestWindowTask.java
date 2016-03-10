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

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;

/**
 * A simple test Samza task that buffers all messages in PROCESS.
 * During WINDOWing every message returns the same key
 * and defines the new value to be x10 the original value.
 * k1 = original key
 * v1 = original value
 * (k1, v1) -> (k1, v1*10)
 */
public class TestWindowTask implements StreamTask, InitableTask, WindowableTask {
    private static final String KAFKA_SYSTEM = "kafka";
    private static final String OUTPUT_STREAM_COUNT_TOPIC = "test";
    private SystemStream outputStream;
    private List<AbstractMap.SimpleEntry> counterList = new LinkedList<AbstractMap.SimpleEntry>();

    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
        outputStream = new SystemStream(KAFKA_SYSTEM, OUTPUT_STREAM_COUNT_TOPIC);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        String key = (String) envelope.getKey();
        Integer value = (Integer) envelope.getMessage();
        counterList.add(new AbstractMap.SimpleEntry<String, Integer>(key, value));
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        for (int i = 0; i < counterList.size(); i++) {
            Integer value = (Integer) counterList.get(i).getValue();
            collector.send(new OutgoingMessageEnvelope(outputStream, counterList.get(i).getKey(), value * 10));
        }
        counterList.clear();
    }
}