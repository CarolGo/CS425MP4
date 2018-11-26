package FunctionTest;

import cs425.crane.function.FunctionLoader;
import cs425.crane.function.Mp4Function;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CompileTest {

    @Test
    void loaderTest() throws Exception {
        File folder = new File(".");
        System.out.println("Search .class file in: " + folder.getAbsolutePath());
        Constructor<? extends Mp4Function> constructor =
                FunctionLoader.loadClass(folder.getAbsolutePath(), "FunctionTest.UniversalFunction");
        assertNotNull(constructor);
        Mp4Function m = constructor.newInstance();
        assertNotNull(m);

        //TODO: Change <Integer, List<Integer>> to a wrapper object
        Function<Integer, List<Integer>> s = m.mp4Task();

        int r = new Random().nextInt(5000);
        List<Integer> rndList = s.apply(r);
        assertEquals(r, rndList.size());
    }

}
