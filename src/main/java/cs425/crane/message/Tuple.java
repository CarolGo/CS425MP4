package cs425.crane.message;

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

    /**
     * Get the data field of the Tuple
     * @return
     */

    public ArrayList<Object> getData() {
        return data;
    }

    /**
     * Get the id of the Tuple
     * @return
     */
    public UUID getId() {
        return id;
    }
}
