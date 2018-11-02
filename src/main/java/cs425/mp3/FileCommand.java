package cs425.mp3;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.LocalDateTime;


/**
 * command send by client to server.
 */

public class FileCommand implements Serializable {

    /**
     * msg type
     */
    private String type;
    /**
     * Target hostName
     */
    private String hostName;

    /**
     * Time that this being send
     */
    private LocalDateTime timestamp = LocalDateTime.now();

    /**
     * version number
     */
    private int versionNum;

    /**
     * File name
     */
    private String fileName;

    /**
     * Initialize File command object
     *
     * @param hostName   Target host
     * @param fileName   Target file name
     * @param versionNum Target file version
     */
    public FileCommand(String type, String hostName, String fileName, int versionNum) {
        this.hostName = hostName;
        this.fileName = fileName;
        this.versionNum = versionNum;
        this.type = type;
    }


    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getHostName() {
        return this.hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getFileName() {
        return this.fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getVersionNum() {
        return this.versionNum;
    }

    public void setVersionNum(int versionNum) {
        this.versionNum = versionNum;
    }

    public static FileCommand parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof FileCommand) return (FileCommand) o;
        return null;
    }

}
