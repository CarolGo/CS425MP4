package cs425.mp3;

import java.util.UUID;
import java.util.Set;

public class FileObject {

    /**
     * name of the file
     */
    private String fileName;

    /**
     * version of the file
     */
    private int version;
    /**
     * replica locations of the file
     */
    private Set<String> replicaLocations;
    /**
     * local path for the file. empty when remote
     */
    private String path;
    /**
     * Initialize File object
     *
     * @param fileName   Target file name
     * @param version Target file version
     */
    public FileObject(String fileName, int version){
        this.fileName = fileName;
        this.version = version;
    }

    /**
     *
     * @return UUID of the file
     */

    public UUID getUUID(){
        return UUID.fromString(this.fileName);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
