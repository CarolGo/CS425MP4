package FunctionTest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.function.Function;

public class BasicFuncTest {
    private static final int numCount = 100_000;

    private static void timing(Function<Void, Void> r) {
        Instant start = Instant.now();
        for (int i = 0; i < 1000; i++) {
            r.apply(null);
        }
        Instant end = Instant.now();
        System.err.println(String.format("Function <%s> running time: %s",
                r.toString(), Duration.between(start, end).toString().toLowerCase()));
    }

    private static Function<Void, Void> genArray() {
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

    private static Function<Void, Void> genLinked() {
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

    public static void main(String... args) throws Exception {
        timing(genArray());
        timing(genLinked());
    }
}
