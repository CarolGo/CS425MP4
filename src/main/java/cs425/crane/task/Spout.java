package cs425.crane.task;

import java.io.Serializable;
import java.util.UUID;

public interface Spout extends Serializable{

    /**
     * Called when a Spout task is initialized with a worker. Prepare the enviornment that the Spout needs to execute.
     */
    void open();

    /**
     * Called when a Spout task is going to be shutdown.
     */
    void close();

    /**
     * Called by crane to generate new tuple.
     */
    void nextTuple();

    /**
     *  Called when a Tuple is fully processed.
     */
    void ack(UUID id);

    /**
     * Called when a Tuple fails to be fully processed.
     */
    void fail(UUID id);

}
