package cs425.crane.function;

import java.util.List;
import java.util.function.Function;

public interface Mp4Function {

    /**
     * An interface for custom function in MP3
     *
     * @return A universal function
     */
    //TODO: Change <Integer, List<Integer>> to a wrapper object
    Function<Integer, List<Integer>> mp4Task();

}
