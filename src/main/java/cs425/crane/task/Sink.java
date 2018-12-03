package cs425.crane.task;

import cs425.crane.message.Tuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.UUID;

public interface Sink extends Serializable {
    /**
     * Called when a Sink task is initialized with a worker. Prepare the environment that Sink needs to execute.
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
     * Called when a Tuple is processed by the Sink.
     */
    void ack(UUID id);

    /**
     * Called when a Tuple fails to be processed by the Sink.
     */
    void fail(UUID id);

    static Sink parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof Sink) return (Sink) o;
        return null;
    }

}
