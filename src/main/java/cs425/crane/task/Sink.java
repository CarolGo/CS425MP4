package cs425.crane.task;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.UUID;

public interface Sink extends Bolt {

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
