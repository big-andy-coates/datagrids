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
public class SyntheticIndexAwareExtractor implements IndexAwareExtractor {
    @PortableProperty(value = 1)
    private ValueExtractor syntheticExtractor;

    @PortableProperty(value = 2)
    private ValueExtractor actualExtractor;

    @PortableProperty(value = 3)
    private EntrySequencer actualKeyCalculator;

    @SuppressWarnings("UnusedDeclaration")      // Used by Coherence
    @Deprecated                                 // Only
    public SyntheticIndexAwareExtractor() {
    }

    public SyntheticIndexAwareExtractor(ValueExtractor syntheticExtractor, ValueExtractor actualExtractor, EntrySequencer actualKeyCalculator) {
        Validate.notNull(syntheticExtractor);
        Validate.notNull(actualExtractor);
        Validate.notNull(actualKeyCalculator);

        this.syntheticExtractor = syntheticExtractor;
        this.actualExtractor = actualExtractor;
        this.actualKeyCalculator = actualKeyCalculator;
    }

    @Override
    public MapIndex createIndex(boolean ordered, Comparator comparator, Map rawMapIndex, BackingMapContext context) {
        //noinspection unchecked,UnnecessaryLocalVariable
        Map<ValueExtractor, MapIndex> mapIndex = rawMapIndex;
        if (mapIndex.containsKey(syntheticExtractor)) {
            throw new IllegalArgumentException("Repetitive addIndex call for " + this);
        }

        MapIndex index = new SyntheticIndex(syntheticExtractor, actualExtractor, actualKeyCalculator, ordered, comparator, context);
        mapIndex.put(syntheticExtractor, index);
        return index;
    }

    @Override
    public MapIndex destroyIndex(Map mapIndex) {
        return (MapIndex) mapIndex.remove(syntheticExtractor);
    }

    @Override
    public Object extract(Object o) {
        throw new UnsupportedOperationException();
    }
}
