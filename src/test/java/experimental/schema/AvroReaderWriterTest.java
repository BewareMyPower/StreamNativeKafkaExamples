package experimental.schema;

import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.reflect.ReflectData;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AvroReaderWriterTest {

    @Test
    public void testSerDes() throws IOException  {
        var schema = new ReflectData.AllowNull().getSchema(User.class);
        System.out.println(schema.toString(true));

        var originalUser = new User("abc", 18);
        var writer = new AvroWriter<>(schema);
        var bytes = writer.write(originalUser);
        // [36, 2, 6, 97, 98, 99]
        System.out.println(Arrays.toString(bytes));

        var reader = new AvroReader<>(schema);
        var user = reader.read(bytes);
        Assert.assertEquals(user, originalUser);
    }
}
