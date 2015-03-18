package theduderog.samza.tasks;

import theduderog.schemas.*;

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

        FortuneRequest request = (FortuneRequest) envelope.getMessage();

        String fortune = "Unknown";
        double certainty = 0.0;
        if (request.getFirstName().equals("Ragnar")) {
            fortune = "The sons of Ragnar Lothbrok will be spoken of as long as men have tongues to speak.";
        }
        else if (request.getFirstName().equals("Lagartha")) {
            if (request.getQuestion().startsWith("Will")) {
                fortune = "I cannot see another child no matter how far i look";
                certainty = 0.5;
            }
            else {
                fortune = "I see a harvest celebrated in blood. I see a trickster whose weapon cleaves you. I see a city made of marble and a burning, broiling ocean.";
                certainty = 0.9;
            }
        }

        Headers header = Headers.newBuilder()
                .setTimestamp(request.header.getTimestamp())
                .build();
        Fortune fortuneRec = Fortune.newBuilder()
                .setHeader(header)
                .setAnswer(fortune)
                .setFirstName(request.getFirstName())
                .setLastName(request.getLastName())
                .setQuestion(request.getQuestion())
                .setDegreeOfCertainty(certainty)
                .build();
        collector.send(new OutgoingMessageEnvelope(outStream, fortuneRec));
    }
}