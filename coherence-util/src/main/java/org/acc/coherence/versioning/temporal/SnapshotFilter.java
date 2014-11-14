package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;
import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.util.*;

/**
 * @author datalorax - 14/11/2014.
 */
@Portable
public class SnapshotFilter implements IndexAwareFilter, Serializable {
    private static final long serialVersionUID = -8801572239569206378L;

    @PortableProperty(value = 1)
    private TemporalExtractor extractor;

    @PortableProperty(value = 2)
    private Object snapshot;

    @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
    @Deprecated                             // Only
    public SnapshotFilter() {
    }

    public SnapshotFilter(TemporalExtractor extractor, Object snapshot) {
        Validate.notNull(extractor, "extractor can not be null");
        Validate.notNull(snapshot, "snapshot can not be null");

        this.extractor = extractor;
        this.snapshot = snapshot;
    }

    @Override
    public int calculateEffectiveness(Map indexMap, Set keys) {
        return 1;   // todo(ac): Hard to say without doing all the work
        // - would need to track average timeLine length and then return keys.size() / average
    }

    @Override
    public Filter applyIndex(Map indexMap, Set keys) {
        TemporalIndex index = getIndex(indexMap);

        List<Object> matches = new ArrayList<Object>();
        Iterator it = keys.iterator();
        while (it.hasNext()) {
            Object match = filterEntry(it.next(), index, keys);
            if (match != null) {
                matches.add(match);
            }
        }

        keys.addAll(matches);
        return null;
    }

    @Override
    public boolean evaluateEntry(Map.Entry entry) {
        throw new UnsupportedOperationException();
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

        throw new UnsupportedOperationException("SnapshotFilter requires a matching temporal index that uses the supplied temporal extractor. " +
                "Found: " + index);
    }

    private Object filterEntry(Object fullKey, TemporalIndex index, Set allKeys) {
        final TimeLine timeLine = index.getTimeLine(fullKey);

        final Object floor = timeLine.get(snapshot);

        allKeys.removeAll(timeLine.keySet());

        return floor;
    }
}
