package org.acc.coherence.versioning.temporal;

import com.tangosol.io.Serializer;
import com.tangosol.io.pof.PofContext;
import com.tangosol.io.pof.reflect.PofNavigator;
import com.tangosol.io.pof.reflect.PofValue;
import com.tangosol.io.pof.reflect.PofValueParser;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.*;
import com.tangosol.util.extractor.PofExtractor;
import org.apache.commons.lang3.Validate;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom index that builds a per business key time-line of versions.
 */
public class TemporalIndex implements MapIndex {
    private final TemporalExtractor extractor;
    private final Serializer serialiser;
    private final Map<Object, TimeLine> timeLineIndex;

    /**
     * Create a TemporalIndex - normally used by way of {@link org.acc.coherence.versioning.temporal.TemporalExtractor}
     *
     * @param extractor  The temporal extractor to use to build the index
     * @param context    The map context for the cache on which the index is to be built.
     */
    public TemporalIndex(TemporalExtractor extractor, BackingMapContext context) {
        Validate.notNull(extractor);

        this.extractor = extractor;
        this.serialiser = context.getManagerContext().getCacheService().getSerializer();
        this.timeLineIndex = new HashMap<Object, TimeLine>();

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
        return null;
    }

    @Override
    public void insert(Map.Entry entry) {
        final TimeLine timeLine = getTimeLine(entry, true);
        addToTimeLine(entry, timeLine);
        // Todo(ac): need to support multiple versions on same time?
    }

    @Override
    public void update(Map.Entry entry) {
        throw new UnsupportedOperationException();  // Todo(ac):
    }

    @Override
    public void delete(Map.Entry entry) {
        final TimeLine timeLine = getTimeLine(entry, true);
        if (timeLine == null) {
            throw new IllegalStateException("Unknown temporal key deleted: " + extractBusinessKeyFromEntry(entry) +
                    ". Full coherence key: " + entry.getKey());
        }

        removeFromTimeLine(entry, timeLine);
    }

    public TimeLine getTimeLine(Object fullKey) {
        final Object businessKey = extractBusinessKeyFromKey(fullKey);
        TimeLine timeLine = getTimeLine(businessKey, false);
        if (timeLine == null) {
            throw new IllegalStateException("Unknown temporal key: " + businessKey + ". Coherence key: " + fullKey);
        }
        return timeLine;
    }

    private TimeLine getTimeLine(Map.Entry entry, boolean createIfNeeded) {
        final Object businessKey = extractBusinessKeyFromEntry(entry);
        return getTimeLine(businessKey, createIfNeeded);
    }

    private TimeLine getTimeLine(Object businessKey, boolean createIfNeeded) {
        TimeLine timeLine = timeLineIndex.get(businessKey);
        if (timeLine == null && createIfNeeded) {
            timeLine = new TimeLine();
            timeLineIndex.put(businessKey, timeLine);
        }

        return timeLine;
    }

    private void addToTimeLine(Map.Entry entry, TimeLine timeLine) {
        final Object arrived = InvocableMapHelper.extractFromEntry(extractor.getTimestampExtractor(), entry);
        timeLine.insert(getCoherenceKey(entry), arrived);
    }

    private void removeFromTimeLine(Map.Entry entry, TimeLine timeLine) {
        final Object arrived = InvocableMapHelper.extractFromEntry(extractor.getTimestampExtractor(), entry);
        if (!timeLine.remove(getCoherenceKey(entry), arrived)) {
            throw new IllegalStateException("Unknown arrived time " + arrived +
                    " for temporal key " + extractBusinessKeyFromEntry(entry) +
                    ". Full coherence key: " + entry.getKey());
        }

        if (timeLine.isEmpty()) {
            timeLineIndex.remove(extractBusinessKeyFromEntry(entry));
        }
    }

    private Object extractBusinessKeyFromEntry(Map.Entry entry) {
        return InvocableMapHelper.extractFromEntry(extractor.getKeyExtractor(), entry);
    }

    private Object extractBusinessKeyFromKey(Object fullKey) {
        if (extractor.getKeyExtractor() instanceof PofExtractor) {
            PofExtractor keyExtractor = (PofExtractor) extractor.getKeyExtractor();
            PofNavigator navigator = keyExtractor.getNavigator();
            PofValue pofValue = PofValueParser.parse((Binary) fullKey, (PofContext) serialiser);
            return navigator.navigate(pofValue).getValue();
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
