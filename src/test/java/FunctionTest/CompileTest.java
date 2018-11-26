package FunctionTest;

import cs425.crane.function.Mp4Function;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.function.Function;

public class CompileTest {

    private static Mp4Function parseClass(File dirRoot, String classFileName) throws IOException {
        File f = new File(dirRoot, classFileName);
        try (URLClassLoader cl = URLClassLoader.newInstance(new URL[]{f.toURI().toURL()})) {
            classFileName = classFileName.split("\\.")[0];
            return (Mp4Function) Class.forName(classFileName, true, cl).newInstance();
        } catch (ReflectiveOperationException e) {
            // Class not found or can not new
            return null;
        }
    }

    private static String getClassPackage(String errorMsg) {
        int startIndex = errorMsg.lastIndexOf(" ") + 1;
        int endIndex = errorMsg.length() - 1;

        String classPackage = errorMsg.substring(startIndex, endIndex);
        return classPackage.replace('/', '.');
    }

    public static void main(String... args) throws Exception {
        File folder = new File("target\\test-classes");

        Mp4Function m = parseClass(folder, "CompileClassTest.class");
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
