package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.PofContext;
import com.tangosol.io.pof.reflect.PofValue;
import com.tangosol.io.pof.reflect.PofValueParser;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.*;
import com.tangosol.util.extractor.PofExtractor;
import org.apache.commons.lang3.Validate;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Custom index that builds a per business key time-line of versions.
 */
public class TemporalIndex implements MapIndex {
    private final TemporalExtractor extractor;
    private final BackingMapContext context;
    private final Map<Object, TreeMap<Object, Object>> timeLineIndex;
    private final Comparator comparator;

    /**
     * Create a TemporalIndex - normally used by way of {@link org.acc.coherence.versioning.temporal.TemporalExtractor}
     *
     * @param extractor  The temporal extractor to use to build the index
     * @param comparator Optional comparator to use for ordering the reverse index
     * @param context    The map context for the cache on which the index is to be built.
     */
    public TemporalIndex(TemporalExtractor extractor, Comparator comparator, BackingMapContext context) {
        Validate.notNull(extractor);
        Validate.notNull(context);

        this.extractor = extractor;
        this.context = context;
        this.comparator = comparator;
        this.timeLineIndex = new HashMap<Object, TreeMap<Object, Object>>();

        // Todo(ac): compare performance of SegmentedHashMap and HashMap for reverse indexes
    }

    @Override
    public ValueExtractor getValueExtractor() {
        return extractor;
    }

    @Override
    public boolean isPartial() {
        return false;
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public Map getIndexContents() {
        throw new UnsupportedOperationException("Temporal Indexes can only be used via Temporal Filters");
    }

    @Override
    public Object get(Object o) {
        throw new UnsupportedOperationException("Temporal Indexes can only be used via Temporal Filters");
    }

    @Override
    public Comparator getComparator() {
        return comparator;
    }

    @Override
    public void insert(Map.Entry entry) {
        final TreeMap<Object, Object> timeLine = getTimeLine(entry, true);
        addToTimeLine(entry, timeLine);
        // Todo(ac): need to support multiple versions on same time?
    }

    @Override
    public void update(Map.Entry entry) {
        throw new UnsupportedOperationException();  // Todo(ac):
    }

    @Override
    public void delete(Map.Entry entry) {
        final TreeMap<Object, Object> timeLine = getTimeLine(entry, true);
        if (timeLine == null) {
            throw new IllegalStateException("Unknown temporal key deleted: " + extractTemporalKeyFromEntry(entry) +
                    ". Full coherence key: " + entry.getKey());
        }

        removeFromTimeLine(entry, timeLine);
    }

    public TreeMap<Object, Object> getTimeLine(Object fullKey) {
        final Object temporalKey = extractTemporalKeyFromKey(fullKey);
        TreeMap<Object, Object> timeLine = getTimeLine(temporalKey, false);
        if (timeLine == null) {
            throw new IllegalStateException("Unknown temporal key: " + temporalKey + ". Coherence key: " + fullKey);
        }
        return timeLine;
    }

    private TreeMap<Object, Object> getTimeLine(Map.Entry entry, boolean createIfNeeded) {
        final Object temporalKey = extractTemporalKeyFromEntry(entry);
        return getTimeLine(temporalKey, createIfNeeded);
    }

    private TreeMap<Object, Object> getTimeLine(Object temporalKey, boolean createIfNeeded) {
        TreeMap<Object, Object> timeLine = timeLineIndex.get(temporalKey);
        if (timeLine == null && createIfNeeded) {
            timeLine = new TreeMap<Object, Object>();
            timeLineIndex.put(temporalKey, timeLine);
        }

        return timeLine;
    }

    private void addToTimeLine(Map.Entry entry, TreeMap<Object, Object> timeLine) {
        final Object arrived = InvocableMapHelper.extractFromEntry(extractor.getArrivedExtractor(), entry);
        timeLine.put(arrived, getCoherenceKey(entry));
    }

    private void removeFromTimeLine(Map.Entry entry, TreeMap<Object, Object> timeLine) {
        final Object arrived = InvocableMapHelper.extractFromEntry(extractor.getArrivedExtractor(), entry);
        if (timeLine.remove(arrived) == null) {
            throw new IllegalStateException("Unknown arrived time " + arrived +
                    " for temporal key " + extractTemporalKeyFromEntry(entry) +
                    ". Full coherence key: " + entry.getKey());
        }

        if (timeLine.isEmpty()) {
            timeLineIndex.remove(extractTemporalKeyFromEntry(entry));
        }
    }

    private Object extractTemporalKeyFromEntry(Map.Entry entry) {
        return InvocableMapHelper.extractFromEntry(extractor.getKeyExtractor(), entry);
    }

    private Object extractTemporalKeyFromKey(Object fullKey) {
        if (extractor.getKeyExtractor() instanceof PofExtractor) {
            PofExtractor keyExtractor = (PofExtractor) extractor.getKeyExtractor();
            PofValue pofValue = PofValueParser.parse((Binary) fullKey, (PofContext) context.getManagerContext().getCacheService().getSerializer());
            return keyExtractor.getNavigator().navigate(pofValue).getValue();
        }
        return extractor.getKeyExtractor().extract(fullKey);
    }

    private static Object getCoherenceKey(Map.Entry entry) {
        if (entry == null) {
            return null;
        }

        return entry instanceof BinaryEntry ? ((BinaryEntry) entry).getBinaryKey() : entry.getKey();
    }
}
