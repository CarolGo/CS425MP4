package cs425.crane.function;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.function.Function;

/**
 * A serializable functional interface
 */
@FunctionalInterface
public interface SeFunction<Y, C> extends Serializable, Function<Y, C> {

    static SeFunction parseFromStream(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object o = in.readObject();
        if (o instanceof SeFunction) return (SeFunction) o;
        return null;
    }

}
