package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.IndexAwareExtractor;
import org.apache.commons.lang3.Validate;

import java.util.Comparator;
import java.util.Map;

/**
 * Created by andy on 28/06/2014.
 */
@Portable
public class TemporalExtractor implements IndexAwareExtractor {
    @PortableProperty(value = 1)
    private ValueExtractor keyExtractor;

    @PortableProperty(value = 2)
    private ValueExtractor arrivedExtractor;

    @SuppressWarnings("UnusedDeclaration")      // Used by Coherence
    @Deprecated                                 // Only
    public TemporalExtractor() {
    }

    /**
     * @param keyExtractor     The extractor to extract the temporal key from the coherence key
     * @param arrivedExtractor The extractor to extract the arrived_at temporal property of an entry.
     */
    public TemporalExtractor(ValueExtractor keyExtractor, ValueExtractor arrivedExtractor) {
        Validate.notNull(keyExtractor);
        Validate.notNull(arrivedExtractor);

        this.keyExtractor = keyExtractor;
        this.arrivedExtractor = arrivedExtractor;
    }

    public ValueExtractor getKeyExtractor() {
        return keyExtractor;
    }

    public ValueExtractor getArrivedExtractor() {
        return arrivedExtractor;
    }

    @Override
    public MapIndex createIndex(boolean ordered, Comparator comparator, Map rawMapIndex, BackingMapContext context) {
        Validate.isTrue(ordered, "Temporal indexes must be ordered");

        //noinspection unchecked,UnnecessaryLocalVariable
        Map<ValueExtractor, MapIndex> mapIndex = rawMapIndex;
        if (mapIndex.containsKey(this)) {
            throw new IllegalArgumentException("Repetitive addIndex call for " + this);
        }

        MapIndex index = new TemporalIndex(this, comparator, context);
        mapIndex.put(this, index);
        return index;
    }

    @Override
    public MapIndex destroyIndex(Map mapIndex) {
        return (MapIndex) mapIndex.remove(this);
    }

    @Override
    public Object extract(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemporalExtractor that = (TemporalExtractor) o;

        if (arrivedExtractor != null ? !arrivedExtractor.equals(that.arrivedExtractor) : that.arrivedExtractor != null) return false;
        if (keyExtractor != null ? !keyExtractor.equals(that.keyExtractor) : that.keyExtractor != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = keyExtractor != null ? keyExtractor.hashCode() : 0;
        result = 31 * result + (arrivedExtractor != null ? arrivedExtractor.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TemporalExtractor{" +
                "keyExtractor=" + keyExtractor +
                ", arrivedExtractor=" + arrivedExtractor +
                '}';
    }
}
