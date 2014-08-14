package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

import java.util.*;

/**
 * @author Andy Coates.
 */
@Portable
public class SnapshotFilter implements IndexAwareFilter {
    @PortableProperty(value = 1)
    private TemporalExtractor extractor;

    @PortableProperty(value = 2)
    private Object snapshot;

    @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
    @Deprecated                             // Only
    public SnapshotFilter() {
    }

    public SnapshotFilter(TemporalExtractor extractor, Object snapshot) {
        this.extractor = extractor;
        this.snapshot = snapshot;
    }

    @Override
    public int calculateEffectiveness(Map indexMap, Set keys) {
        getIndex(indexMap);
        return 1;   // todo(ac): Hard to say without doing all the work - would need to track average timeLine length and then return keys.size() /
        // average
    }

    @Override
    public Filter applyIndex(Map indexMap, Set keys) {
        TemporalIndex index = getIndex(indexMap);

        List<Object> matches = new ArrayList<Object>();
        for (Object key : keys) {
            Object match = filterEntry(key, index, keys);
            if (match != null) {
                matches.add(match);
            }
        }

        keys.addAll(matches);
        return null;
    }

    @Override
    public boolean evaluateEntry(Map.Entry entry) {
        // Todo(aC):
        return false;
    }

    @Override
    public boolean evaluate(Object o) {
        throw new UnsupportedOperationException();
    }

    private TemporalIndex getIndex(Map indexMap) {
        Object index = indexMap.get(extractor);
        if (index instanceof TemporalIndex) {
            return (TemporalIndex) index;
        }

        throw new UnsupportedOperationException("SnapshotFilter requires a matching temporal index that uses the supplied temporal extractor");
    }

    private Object filterEntry(Object fullKey, TemporalIndex index, Set allKeys) {
        final TreeMap<Object, Object> timeLine = index.getTimeLine(fullKey);

        final Map.Entry<Object, Object> floor = timeLine.floorEntry(snapshot);

        allKeys.removeAll(timeLine.values());

        return floor == null ? null : floor.getValue();
    }
}
