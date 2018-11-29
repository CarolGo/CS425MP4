package cs425.crane.message;

import java.io.Serializable;
import java.util.UUID;
import java.io.ObjectInputStream;
import java.io.IOException;

/**
 * AckMessage is used by a Sink to inform spout that whether a particular Tuple identified by the UUID is fully processed or used by a worker to
 * inform master that whether a particular task is assigned.
 */
public class AckMessage implements Serializable {
    private final UUID id;
    private final boolean isFinished;

    /**
     * initialize the AckMessage.
     * @param id            UUID for a particular Tuple
     * @param isFinished    whether the Tuple is fully processed or whether a task is assigned
     */
    public AckMessage(UUID id, boolean isFinished){
        this.id = id;
        this.isFinished = isFinished;
    }

    public UUID getId() {
        return id;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public static AckMessage parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof AckMessage) return (AckMessage) o;
        return null;
    }
}
