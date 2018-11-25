package cs425.crane.task;

import java.io.Serializable;
import cs425.crane.message.Tuple;

public interface Bolt extends Serializable {
    /**
     * Called when a Bolt task is initialized with a worker. Prepare the environment that Bolt needs to execute.
     */
    void prepare();

    /**
     * Called by crane to process a new tuple.
     * @param tuple
     */
    void execute(Tuple tuple);

    /**
     *  called when a Bolt task is going to be shotdown.
     */
    void cleanUp();

}
