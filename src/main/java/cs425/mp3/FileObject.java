package cs425.mp3;

import java.io.Serializable;
import java.util.Set;

public class FileObject implements Serializable {

    /**
     * name of the file
     */
    private String id;

    /**
     * version of the file
     */
    private int version;
    /**
     * replica locations of the file
     */
    private Set<String> replicaLocations;

    /**
     * Initialize File object
     *
     * @param version Target file version
     */
    public FileObject(int version) {
        this.id = Util.generateUuid();
        this.version = version;
    }

    /**
     * @return UUID of the file
     */

    public String getUUID() {
        return this.id;
    }


    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Set<String> getReplicaLocations() {
        return replicaLocations;
    }

    public void setReplicaLocations(Set<String> replicaLocations) {
        this.replicaLocations = replicaLocations;
    }

}
