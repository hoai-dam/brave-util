package brave.cache.util;

import java.lang.reflect.Array;
import java.util.Collection;

public class CollectionUtil {

    public static <T> T[] toArray(Class<T> c, Collection<T> collection) {
        //noinspection unchecked
        return collection.toArray((T[]) Array.newInstance(c, collection.size()));
    }

}
