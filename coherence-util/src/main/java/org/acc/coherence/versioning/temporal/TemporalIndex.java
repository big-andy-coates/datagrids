package org.acc.coherence.versioning.temporal;

import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.MapTrigger;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.comparator.SafeComparator;
import org.acc.coherence.versioning.util.InvocableMapHelper;
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
    private final Comparator<Object> versionComparator;

    /**
     * Create a TemporalIndex - normally used by way of {@link org.acc.coherence.versioning.temporal.TemporalExtractor}
     *
     * @param extractor The temporal extractor to use to build the index
     * @param context   The map context for the cache on which the index is to be built.
     */
    public TemporalIndex(TemporalExtractor extractor, BackingMapContext context) {
        Validate.notNull(extractor);

        this.extractor = extractor;
        this.serialiser = context.getManagerContext().getCacheService().getSerializer();
        this.timeLineIndex = new HashMap<Object, TimeLine>();
        this.versionComparator = createVersionComparator(extractor.getVersionExtractor(), serialiser);

        // Todo(ac): compare performance of SegmentedHashMap and HashMap for reverse indexes
    }

    @Override
    public ValueExtractor getValueExtractor() {
        return extractor;
    }

    @Override
    public boolean isPartial() {
        throw new UnsupportedOperationException("Temporal Indexes can only be used via Temporal Filters");
    }

    @Override
    public boolean isOrdered() {
        throw new UnsupportedOperationException("Temporal Indexes can only be used via Temporal Filters");
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
        throw new UnsupportedOperationException("Temporal Indexes can only be used via Temporal Filters");
    }

    @Override
    public void insert(Map.Entry entry) {
        final TimeLine timeLine = getTimeLine(entry, true);
        addToTimeLine(entry, timeLine);
    }

    @Override
    public void update(Map.Entry entry) {
        throw new UnsupportedOperationException();  // Todo(ac): not really needed if data is immutable...
    }

    @Override
    public void delete(Map.Entry entry) {
        final TimeLine timeLine = getTimeLine(entry, false);
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
            timeLine = new TimeLine(versionComparator);
            timeLineIndex.put(businessKey, timeLine);
        }

        return timeLine;
    }

    private void addToTimeLine(Map.Entry entry, TimeLine timeLine) {
        final Object arrived = InvocableMapHelper.extractFromEntry(extractor.getTimestampExtractor(), entry);
        if (arrived == null) {
            throw new IllegalArgumentException("Failed to extract timestamp from supplied entry: " + entry +
                    ", extractor: " + extractor.getTimestampExtractor());
        }
        timeLine.insert(getCoherenceKey(entry), arrived);
    }

    private void removeFromTimeLine(Map.Entry entry, TimeLine timeLine) {
        final Object arrived = extractTimestampFromOriginalValue(entry);
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
        Object businessKey = InvocableMapHelper.extractFromEntry(extractor.getBusinessKeyExtractor(), entry);
        if (businessKey == null) {
            throw new IllegalArgumentException("Failed to extract the business key from the supplied entry: " + entry +
                    ", extractor: " + extractor.getBusinessKeyExtractor());
        }
        return businessKey;
    }

    private Object extractBusinessKeyFromKey(Object fullKey) {
        Object businessKey = InvocableMapHelper.extractFromObject(extractor.getBusinessKeyExtractor(), fullKey, serialiser);
        if (businessKey == null) {
            throw new IllegalArgumentException("Failed to extract the business key from the supplied key: " + fullKey +
                    ", extractor: " + extractor.getBusinessKeyExtractor());
        }
        return businessKey;
    }

    private Object extractTimestampFromOriginalValue(Map.Entry entry) {
        Object timestamp = InvocableMapHelper.extractOriginalFromEntry(extractor.getTimestampExtractor(), (MapTrigger.Entry) entry);
        if (timestamp == null) {
            throw new IllegalArgumentException("Failed to extract the original timestamp from the supplied entry: " + entry +
                    ", extractor: " + extractor.getTimestampExtractor());
        }
        return timestamp;
    }

    private static Object getCoherenceKey(Map.Entry entry) {
        return entry instanceof BinaryEntry ? ((BinaryEntry) entry).getBinaryKey() : entry.getKey();
    }

    private static Comparator<Object> createVersionComparator(final ValueExtractor versionExtractor, final Serializer serialiser) {
        if (versionExtractor == null) {
            return null;    // Natural ordering of full key
        }

        return new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                return SafeComparator.compareSafe(null, extract(o1), extract(o2));
            }

            private Comparable extract(Object o) {
                Object version = InvocableMapHelper.extractFromObject(versionExtractor, o, serialiser);
                if (version == null) {
                    throw new IllegalArgumentException("Failed to extract the version from the supplied object: " + o +
                            ", extractor: " + versionExtractor);
                }
                if (version instanceof Comparable) {
                    return (Comparable) version;
                }

                throw new IllegalArgumentException("Versions must be comparable: " + version + ", extractor: " + versionExtractor);
            }
        };
    }
}
