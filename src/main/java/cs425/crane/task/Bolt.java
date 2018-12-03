package cs425.crane.task;

import cs425.crane.message.Tuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public interface Bolt extends Serializable {
    /**
     * Called when a Bolt task is initialized with a worker. Prepare the environment that Bolt needs to execute.
     */
    void prepare();

    /**
     * Called by crane to process a received tuple.
     *
     * @param tuple
     */
    void process(Tuple tuple);

    /**
     * called when a Bolt task is going to be shotdown.
     */
    void cleanUp();

    /**
     * Called by crane to generate new tuple.
     * @return Tuple that generated
     */
    Tuple nextTuple();

    static Bolt parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof Bolt) return (Bolt) o;
        return null;
    }

}
