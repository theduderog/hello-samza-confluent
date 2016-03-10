package theduderog.samza;

import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.test.harness.TaskUnitTestHarness;
import org.junit.Test;
import theduderog.schemas.Fortune;
import theduderog.schemas.FortuneRequest;
import theduderog.schemas.Headers;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by georgeli on 16-03-11.
 */
public class SeerTaskTest {

    final String Class_Task_Name = "theduderog.samza.tasks.SeerTask";
    final String Dummy_Key = "";

    //{"header": {"timestamp": 1410847837000}, "first_name": "Ragnar", "last_name": "Lothbrok", "question": "The gods have given me sons, as they promised, but I want to know what the future holds. What will become of them?"}

    static FortuneRequest sampleInput(){
        Headers header = Headers.newBuilder()
                .setTimestamp(1410847837000L)
                .build();
        FortuneRequest.Builder builder = FortuneRequest.newBuilder();
        builder.setFirstName("Ragnar").setLastName("Lothbrok")
                .setQuestion("The gods have given me sons, as they promised, but I want to know what the future holds. What will become of them?")
                .setHeader(header);

        return builder.build();
    }

    @Test
    public void fullAction() throws Exception {
        Map<String, String> configMap = new HashMap<String, String>();
        configMap.put("out", "fortunes");

        Config testConfig = new MapConfig(configMap);

        TaskUnitTestHarness<String, GenericRecord> testProcessTask =
                new TaskUnitTestHarness<String, GenericRecord>(Class_Task_Name, false, true);
        testProcessTask.start(testConfig);
        assertTrue(testProcessTask.isStarted());

        assertEquals(0, testProcessTask.getResult().size());
        testProcessTask.inject(Dummy_Key, sampleInput());
        assertEquals(1, testProcessTask.getResult().size());
        testProcessTask.inject(Dummy_Key, sampleInput());
        assertEquals(2, testProcessTask.getResult().size());

        Fortune outMsg = (Fortune)testProcessTask.getResult().get(1).getMessage();

        assertEquals(outMsg.getAnswer().toString(),
                "The sons of Ragnar Lothbrok will be spoken of as long as men have tongues to speak.");
    }
}
