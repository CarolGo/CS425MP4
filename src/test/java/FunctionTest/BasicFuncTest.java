package FunctionTest;

import cs425.crane.function.SeFunction;

import java.io.*;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

public class BasicFuncTest {
    private static final int numCount = 100_000;

    private static void timing(SeFunction<Void, Void> r) {
        Instant start = Instant.now();
        for (int i = 0; i < 1000; i++) {
            r.apply(null);
        }
        Instant end = Instant.now();
        System.err.println(String.format("Function <%s> running time: %s",
                r.toString(), Duration.between(start, end).toString().toLowerCase()));
    }

    private static SeFunction<Void, Void> genArray() {
        return vv -> {
            Random r = new Random();
            ArrayList<Integer> a = new ArrayList<>();
            for (int i = 0; i < numCount; i++) {
                a.add(r.nextInt());
            }
            int t = a.get(a.size() - 1);
            return null;
        };
    }

    private static SeFunction<Void, Void> genLinked() {
        return vv -> {
            Random r = new Random();
            LinkedList<Integer> a = new LinkedList<>();
            for (int i = 0; i < numCount; i++) {
                a.add(r.nextInt());
            }
            int t = a.getLast();
            return null;
        };
    }

    // Ref: https://gist.github.com/eshrubs/29df2d808c15ed5333e709e9e34eafd7
    private static SerializedLambda getSerialized(SeFunction s) throws Exception {
        // There is a private method made by the compiler called ${writeReplace}
        // that converts this lambda to a SerializedLambda.
        // Expose that method and run it!
        Method writeReplace = AccessController.doPrivileged((PrivilegedExceptionAction<Method>) () -> {
            Method method = s.getClass().getDeclaredMethod("writeReplace");
            method.setAccessible(true);
            return method;
        });
        SerializedLambda res = (SerializedLambda) writeReplace.invoke(s);
        return res;
    }

    private static SeFunction<Void, Void> readFuncFromFile(String fp) throws Exception {
        SeFunction<Void, Void> res = null;
        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fp)))) {
            res = SeFunction.parseFromStream(ois);
        }
        return res;
    }

    public static void main(String... args) throws Exception {
        try (ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("f1_arr.f")))) {
            oos.writeObject(getSerialized(genArray()));
        }
        try (ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("f1_lk.f")))) {
            oos.writeObject(getSerialized(genLinked()));
        }

        timing(readFuncFromFile("f1_arr.f"));
        timing(readFuncFromFile("f1_lk.f"));
    }
}
