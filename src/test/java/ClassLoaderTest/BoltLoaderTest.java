package ClassLoaderTest;


import cs425.crane.message.Tuple;
import cs425.crane.task.Sink;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import cs425.crane.JavaClassLoader;
import cs425.crane.task.Bolt;

import java.util.UUID;

public class BoltLoaderTest {

    @Test
    //cs425.crane.applications.TopTenSink
    void LoaderTest() {
        JavaClassLoader javaClassLoader = new JavaClassLoader();
        Sink customSink = (Sink) javaClassLoader.createInstance("cs425.crane.applications.TopTenSink");
    }
}
