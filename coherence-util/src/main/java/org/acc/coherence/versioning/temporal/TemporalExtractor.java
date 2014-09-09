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
    private ValueExtractor businessKeyExtractor;

    @PortableProperty(value = 2)
    private ValueExtractor timestampExtractor;

    @PortableProperty(value = 3)
    private ValueExtractor versionExtractor;

    @SuppressWarnings("UnusedDeclaration")      // Used by Coherence
    @Deprecated                                 // Only
    public TemporalExtractor() {
    }

    // Todo(ac): support multiple versions on same timestamp.

    /**
     * @param businessKeyExtractor The extractor to extract the business key from the full key
     * @param timestampExtractor   The extractor to extract the temporal property of the entry e.g. the created or valid timestamp.
     * @param versionExtractor     An optional extractor to extract the version info from a key.  The extractor must return an type that
     *                             supports the {@link Comparable} interface. If provided the order of versions will defined by the natural order of
     *                             the returned object. If not provided the order of versions is defined by the natural order of the full key. The
     *                             'last' version will be returned where clashes occur.
     */
    public TemporalExtractor(ValueExtractor businessKeyExtractor, ValueExtractor timestampExtractor, ValueExtractor versionExtractor) {
        Validate.notNull(businessKeyExtractor);
        Validate.notNull(timestampExtractor);

        this.businessKeyExtractor = businessKeyExtractor;
        this.timestampExtractor = timestampExtractor;
        this.versionExtractor = versionExtractor;
    }

    public ValueExtractor getBusinessKeyExtractor() {
        return businessKeyExtractor;
    }

    public ValueExtractor getTimestampExtractor() {
        return timestampExtractor;
    }

    public ValueExtractor getVersionExtractor() {
        return versionExtractor;
    }

    @Override
    public MapIndex createIndex(boolean ordered, Comparator comparator, Map rawMapIndex, BackingMapContext context) {
        Validate.isTrue(ordered, "Temporal indexes must be ordered");

        //noinspection unchecked,UnnecessaryLocalVariable
        Map<ValueExtractor, MapIndex> mapIndex = rawMapIndex;
        if (mapIndex.containsKey(this)) {
            throw new IllegalArgumentException("Repetitive addIndex call for " + this);
        }

        MapIndex index = new TemporalIndex(this, context);
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

        TemporalExtractor extractor = (TemporalExtractor) o;

        if (!businessKeyExtractor.equals(extractor.businessKeyExtractor)) return false;
        if (!timestampExtractor.equals(extractor.timestampExtractor)) return false;
        if (versionExtractor != null ? !versionExtractor.equals(extractor.versionExtractor) : extractor.versionExtractor != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = businessKeyExtractor.hashCode();
        result = 31 * result + timestampExtractor.hashCode();
        result = 31 * result + (versionExtractor != null ? versionExtractor.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TemporalExtractor{" +
                "businessKeyExtractor=" + businessKeyExtractor +
                ", timestampExtractor=" + timestampExtractor +
                ", versionExtractor=" + versionExtractor +
                '}';
    }
}
