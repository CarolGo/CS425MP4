package cs425.crane.task;

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

}
