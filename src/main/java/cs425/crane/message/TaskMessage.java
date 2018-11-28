package cs425.crane.message;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.io.ObjectInputStream;
import java.io.IOException;

/**
 * TaskMessage is used by master to assign tasks to different workers or used by worker to request master for task assignment.
 */
public class TaskMessage implements Serializable {

    private final String type;
    private final String src;
    private final String name;
    private final String dest;
    private final String object;
    private LocalDateTime timestamp = LocalDateTime.now();
    private final int port;
    public String getName() {
        return name;
    }

    /**
     * Initialize the create task message with task type, number of threads, source and output
     *
     * @param type   task type
     * @param src    task source
     * @param dest   task destination
     * @param object task source object
     */
    public TaskMessage(String type, String src, String name, String dest, String object, int port) {
        this.type = type;
        this.src = src;
        this.name = name;
        this.dest = dest;
        this.object = object;
        this.port = port;
    }

    public String getType() {
        return type;
    }

    public String getDest() {
        return dest;
    }

    public String getSrc() {
        return src;
    }

    public String getObject() {
        return object;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public int getPort() {
        return port;
    }

    public static TaskMessage parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof TaskMessage) return (TaskMessage) o;
        return null;
    }
}
