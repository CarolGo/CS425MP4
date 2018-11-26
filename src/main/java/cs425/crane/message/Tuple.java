package cs425.crane.message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.UUID;
import java.util.ArrayList;
import java.io.Serializable;


/**
 * Tuple is the main data structure for data transfer between works.
 */
public class Tuple implements Serializable{

    UUID id;
    ArrayList<Object> data = new ArrayList<>();

    /**
     * Initialize the Tuple with a UUID and data values
     *
     * @param id
     * @param vals
     */
    public Tuple(UUID id, Object... vals) {
        this.id = id;
        for (Object o : vals) {
            data.add(o);
        }
    }

    public ArrayList<Object> getData() {
        return data;
    }

    public UUID getId() {
        return id;
    }

    public static Tuple parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof Tuple) return (Tuple) o;
        return null;
    }
}
