package theduderog.samza.serializers;

import kafka.utils.VerifiableProperties;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;

import java.util.Properties;

/**
 * Created by rhoover on 3/17/15.
 */
public class AvroSerdeFactory implements SerdeFactory<Object> {
    @Override
    public AvroSerde getSerde(String s, Config config) {
        final String registryUrl = config.get("rico.schema.registry.url");
        final Properties encoderProps = new Properties();
        encoderProps.setProperty("schema.registry.url", registryUrl);
        final Properties decoderProps = new Properties();
        decoderProps.setProperty("schema.registry.url", registryUrl);
        decoderProps.setProperty("specific.avro.reader", "true");
        return new AvroSerde(new VerifiableProperties(encoderProps), new VerifiableProperties(decoderProps));
    }
}
