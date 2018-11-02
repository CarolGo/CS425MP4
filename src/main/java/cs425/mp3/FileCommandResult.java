package cs425.mp3;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

public class FileCommandResult implements Serializable{

    /**
     * host names for storing and fetch replicas
     */
    private List<String> replicaNodes;
    /**
     * Time that this being send
     */
    private LocalDateTime timestamp = LocalDateTime.now();
    /**
     * command success?
     */
    private boolean hasError = false;


    public void FileCommandResult(List<String> replicaNodes){
        this.replicaNodes = replicaNodes;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public LocalDateTime getTimestamp() {
        return this.timestamp;
    }

    public List<String> getReplicaNodes(){
        return this.replicaNodes;
    }

    public void setReplicaNodes(List<String> replicaNodes){
        this.replicaNodes = replicaNodes;
    }

    public boolean isHasError() {
        return this.hasError;
    }

    public void setHasError(boolean hasError) {
        this.hasError = hasError;
    }



}