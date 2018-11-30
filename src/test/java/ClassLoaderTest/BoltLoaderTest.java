package ClassLoaderTest;


import cs425.crane.message.Tuple;
import org.junit.jupiter.api.Test;
import cs425.crane.JavaClassLoader;
import cs425.crane.task.Bolt;

import java.util.UUID;

public class BoltLoaderTest {

    @Test
    void LoaderTest(){
        JavaClassLoader javaClassLoader = new JavaClassLoader();
        Bolt customBolt = (Bolt)javaClassLoader.createInstance("ClassLoaderTest.CustomBolt");
        customBolt.prepare();
        Tuple t;
        UUID id;
        for(int i = 0; i < 10; i ++){
            id = UUID.randomUUID();
            System.out.println(id.toString());
            t = new Tuple(id, new Integer(i));
            customBolt.execute(t);
        }
        customBolt.cleanUp();
    }

}
