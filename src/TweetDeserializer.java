import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class TweetDeserializer implements Deserializer<Tweet>{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Tweet deserialize(String arg0, byte[] arg1) {
		try{ 
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(arg1));
			return (Tweet) in.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
