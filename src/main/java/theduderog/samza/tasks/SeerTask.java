package theduderog.samza.tasks;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

public class SeerTask implements StreamTask, InitableTask {
    private SystemStream outStream;

    @Override
    public void init(Config cfg, TaskContext ctxt) throws Exception {
        String outTopic = cfg.get("out");
        outStream = new SystemStream("kafka", outTopic);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector,
                        TaskCoordinator coordinator) throws Exception {

        S2PerfEvent event = (FortuneQuestion) envelope.getMessage();
        Headers header = Headers.newBuilder()
                .setTimestamp(event.header.getTimestamp())
                .setCreated("fake time")
                .build();
        S2PerfEnrichedEvent enriched = S2PerfEnrichedEvent.newBuilder()
                .setHeader(header)
                .setReqDurationMs(event.getReqDurationMs())
                .setReqStartTimestampUnix(event.getReqStartTimestampUnix())
                .setSvcName(event.getSvcName())
                .setSvrAppDomain("fakeDomain")
                .setSvrBinary("fakeBinary")
                .setSvrDatacenter("fakeDC")
                .setSvrDbDomain("fakeDBDomain")
                .setSvrEasi(event.getSvrEasi())
                .setSvrHost(event.getSvrHost())
                .setSvrLogicalDbName("fakeLogicalDbName")
                .setSvrLogicalDbSvr("fakeLogicalDBSvr")
                .setSvrMaxProcs(5)
                .setSvrMinProcs(1)
                .setSvrPhysicalDbName("fakeDBName")
                .setSvrPhysicalDbSvr("fakeDbSvr")
                .setSvrProcessId(99999L)
                .setSvrProgram("fakeProgram")
                .setSvrQueue("fakeQueue")
                .setSvrWorkgroup("fakeWkgrp")
                .build();
        collector.send(new OutgoingMessageEnvelope(outStream, enriched));
    }
}