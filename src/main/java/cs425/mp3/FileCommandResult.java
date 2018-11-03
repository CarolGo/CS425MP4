package cs425.mp3;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Set;

public class FileCommandResult implements Serializable {

    /**
     * host names for storing and fetch replicas
     */
    private Set<String> replicaNodes;
    /**
     * Time that this being send
     */
    private LocalDateTime timestamp = LocalDateTime.now();
    /**
     * command success?
     */
    private boolean hasError = false;

    /**
     * version for a file 0 if not exist
     */
    private int version;


    public FileCommandResult(Set<String> replicaNodes, int version) {
        this.replicaNodes = replicaNodes;
        this.version = version;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public LocalDateTime getTimestamp() {
        return this.timestamp;
    }

    public Set<String> getReplicaNodes() {
        return this.replicaNodes;
    }

    public void setReplicaNodes(Set<String> replicaNodes) {
        this.replicaNodes = replicaNodes;
    }

    public boolean isHasError() {
        return this.hasError;
    }

    public void setHasError(boolean hasError) {
        this.hasError = hasError;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public static FileCommandResult parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof FileCommandResult) return (FileCommandResult) o;
        return null;
    }


}
