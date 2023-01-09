package experimental.schema;

import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AvroReaderWriterTest {

    @Test
    public void testSerDes() throws IOException  {
        Schema schema = new ReflectData.AllowNull().getSchema(User.class);
        System.out.println(schema.toString(true));

        User originalUser = new User("abc", 18);
        AvroWriter<User> writer = new AvroWriter<>(schema);
        byte[] bytes = writer.write(originalUser);
        // [36, 2, 6, 97, 98, 99]
        System.out.println(Arrays.toString(bytes));

        AvroReader<User> reader = new AvroReader<>(schema);
        User user = reader.read(bytes);
        Assert.assertEquals(user, originalUser);
    }
}
