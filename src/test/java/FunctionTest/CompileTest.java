package FunctionTest;

import cs425.crane.function.Mp4Function;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.function.Function;

public class CompileTest {

    private static Mp4Function parseClass(File classFilePath, String classPath) throws IOException {
        try (URLClassLoader cl = URLClassLoader.newInstance(new URL[]{classFilePath.toURI().toURL()})) {
            Class<?> rawInput = Class.forName(classPath, true, cl);
            Class<? extends Mp4Function> newClass = rawInput.asSubclass(Mp4Function.class);
            Constructor<? extends Mp4Function> constructor = newClass.getConstructor();
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            // Class not found or can not new etc.
            return null;
        }
    }

    public static void main(String... args) throws Exception {
        File folder = new File("target\\test-classes", "FunctionTest.UniversalFunction.class");

        Mp4Function m = parseClass(folder, "FunctionTest.UniversalFunction");
        if (m == null) {
            System.err.println("Class bad defined");
            return;
        }
        //TODO: Change <Integer, List<Integer>> to a wrapper object
        Function<Integer, List<Integer>> s = m.mp4Task();
        List<Integer> rndList = s.apply(1000);
        System.out.println(String.format("Total lenth: %d\n11th element: %d", rndList.size(), rndList.get(11)));
    }

}
