package org.acc.coherence.index;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.*;

import java.io.Serializable;
import java.util.*;

/**
 * @author datalorax - 04/11/2014.
 */

@Portable
public class IndexContentExtractor implements InvocableMap.ParallelAwareAggregator, Serializable {

    private static final long serialVersionUID = 7058600393076015580L;

    @PortableProperty(value = 1)
    private ValueExtractor indexExtractor;

    @PortableProperty(value = 2)
    private Filter filter;

    @SuppressWarnings("UnusedDeclaration") // For Coherence
    @Deprecated                            // Only
    public IndexContentExtractor() {
    }

    public IndexContentExtractor(ValueExtractor indexExtractor) {
        this(indexExtractor, null);
    }

    public IndexContentExtractor(ValueExtractor indexExtractor, Filter filter) {
        this.indexExtractor = indexExtractor;
        this.filter = filter;
    }

    @Override
    public InvocableMap.EntryAggregator getParallelAggregator() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object aggregateResults(Collection results) {
        if (results.isEmpty()) {
            return Collections.emptySet();
        }
        Set values = new HashSet();
        for (Collection s : (Collection<Collection>)results){
            values.addAll(s);
        }
        return values;
    }

    @Override
    public Object aggregate(Set set) {
        if (set.isEmpty()) {
            return Collections.emptySet();
        }
        final Map indexMap = getIndexMap(set);
        final Set values = getIndexedValues(indexMap);
        filterValues(values);
        return values;
    }

    private Set getIndexedValues(Map indexMap) {
        final MapIndex index = (MapIndex) indexMap.get(indexExtractor);
        if (index == null) {
            throw new IllegalStateException("Index for extractor does not exist: " + indexExtractor);
        }

        final Map contents = index.getIndexContents();
        return contents == null ? Collections.emptySet() : new HashSet<Object>(contents.keySet());
    }

    private void filterValues(Set values) {
        if (filter == null) {
            return;
        }
        for (final Iterator it = values.iterator(); it.hasNext(); ) {
            if (!filter.evaluate(it.next())) {
                it.remove();
            }
        }
    }

    private static Map getIndexMap(Set set) {
        final Object entry = set.iterator().next();
        if (!(entry instanceof BinaryEntry)) {
            throw new UnsupportedOperationException("Only supports binary caches");
        }

        final BinaryEntry binaryEntry = (BinaryEntry) entry;
        return binaryEntry.getBackingMapContext().getIndexMap();
    }
}
