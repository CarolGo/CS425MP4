import cs425.crane.function.Mp4Function;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class CompileClassTest implements Mp4Function {

    private static final Random r = new Random();

    private static List<Integer> genList(int size) {
        List<Integer> res = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            res.add(r.nextInt(10000));
        }
        return res;
    }

    public Function<Integer, List<Integer>> mp4Task() {
        return input -> {
            return genList(input);
        };
    }

}
