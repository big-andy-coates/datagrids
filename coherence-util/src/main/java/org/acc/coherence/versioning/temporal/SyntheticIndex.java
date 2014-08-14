package org.acc.coherence.versioning.temporal;

import com.tangosol.net.BackingMapContext;
import com.tangosol.util.*;
import org.apache.commons.lang3.Validate;

import java.util.*;

/**
 * Custom index capable on indexing synthesised fields e.g. base on other map entries.
 */
public class SyntheticIndex implements MapIndex {
    private final SyntheticExtractor syntheticExtractor;
    private final ValueExtractor actualExtractor;
    private final EntrySequencer entrySequencer;
    private final boolean ordered;
    private final Comparator comparator;
    private final BackingMapContext context;
    private final Map<Object, Set<Object>> reverseIndex;
    private final Map<Object, Object> forwardIndex;

    // Todo(ac): remove - explicit.
    // Todo(ac): Can we extend SimpleMapIndex?
    public SyntheticIndex(SyntheticExtractor syntheticExtractor, ValueExtractor actualExtractor, EntrySequencer entrySequencer,
                          boolean ordered, Comparator<Object> comparator, BackingMapContext context) {
        this(syntheticExtractor, actualExtractor, entrySequencer, ordered, comparator, true, context);
    }

    /**
     * Create a SyntheticIndex - normally used by way of {@link SyntheticIndexAwareExtractor}
     *
     * @param syntheticExtractor The synthetic extractor. Stored in the cache's map of extractor -> index map. It is this extractor that should be passed
     *                           to filters and used in aggregators
     * @param actualExtractor    The actual extractor that is capable of extracting the value from the previous / next entries.
     * @param entrySequencer     The entry sequencer to use to get previous / next entries, based on the current entry being indexed
     * @param ordered            Indicates if the reverse index should be ordered - which allows more efficient greater/less/between style operations
     * @param comparator         Optional comparator to use for ordering the reverse index
     * @param incForward         Indicates if a forward index should be maintained. Forward indexes are required to support update/delete operations on
     *                           the index and to support aggregators calling QueryMap.Entry.extract(syntheticExtractor), which would otherwise fail as the
     *                           extractor is not a true extractor. The forward index can be excluded, to save space, if update/delete operations and
     *                           aggregation are not required.
     * @param context            The map context for the cache on which the index is to be built.
     */
    public SyntheticIndex(SyntheticExtractor syntheticExtractor, ValueExtractor actualExtractor, EntrySequencer entrySequencer,
                          boolean ordered, Comparator<Object> comparator, boolean incForward, BackingMapContext context) {
        Validate.notNull(syntheticExtractor);
        Validate.notNull(actualExtractor);
        Validate.notNull(entrySequencer);
        Validate.notNull(context);

        this.syntheticExtractor = syntheticExtractor;
        this.actualExtractor = actualExtractor;
        this.entrySequencer = entrySequencer;
        this.ordered = ordered;
        this.comparator = comparator;
        this.context = context;
        this.reverseIndex = ordered ? new TreeMap<Object, Set<Object>>(comparator) : new HashMap<Object, Set<Object>>();
        this.forwardIndex = new SegmentedHashMap();
    }

    @Override
    public ValueExtractor getValueExtractor() {
        return syntheticExtractor;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public boolean isPartial() {
        // Partial by definition i.e. not all entries are in index. Value is extracted from prev/next, so at least one end can not have a index entry.
        return true;
    }

    @Override
    public Map getIndexContents() {
        return reverseIndex;
    }

    @Override
    public Object get(Object key) {
        Map map = this.forwardIndex;
        if (map == null) {
            return NO_VALUE;
        }
        Object oValue = map.get(key);
        return (oValue == null) && (!map.containsKey(key)) ? NO_VALUE : oValue;
    }

    @Override
    public Comparator getComparator() {
        return comparator;
    }

    @Override
    public void insert(Map.Entry entry) {
        insert(getKey(entry), extractIndexedAttribute(entrySequencer.getNext(entry, context)));
        insert(getKey(entrySequencer.getPrevious(entry, context)), extractIndexedAttribute(entry));
    }

    @Override
    public void update(Map.Entry entry) {
        throw new UnsupportedOperationException("entries should never be updated");
    }

    @Override
    public void delete(Map.Entry entry) {
        delete(getKey(entry), extractIndexedAttribute(entrySequencer.getNext(entry, context)));
        delete(getKey(entrySequencer.getPrevious(entry, context)), extractIndexedAttribute(entry));
    }

    private void insert(Object key, Object value) {
        if (key == null || value == null) {
            return; // No entry or no value
        }

        Set<Object> keys = reverseIndex.get(value);
        if (keys == null) {
            keys = new HashSet<Object>();
            reverseIndex.put(value, keys);
        }

        keys.add(key);
    }

    private void delete(Object key, Object value) {
        if (key == null || value == null) {
            return;
        }

        Set<Object> keys = reverseIndex.get(value);
        if (keys == null) {
            return;
        }

        if (!keys.remove(key)) {
            return;
        }

        if (keys.isEmpty()) {
            reverseIndex.remove(value);
        }
    }

    private Object extractIndexedAttribute(Map.Entry entry) {
        if (entry == null) {
            return null;
        }

        return InvocableMapHelper.extractFromEntry(actualExtractor, entry);
    }

    private static Object getKey(Map.Entry entry) {
        if (entry == null) {
            return null;
        }

        return entry instanceof BinaryEntry ? ((BinaryEntry) entry).getBinaryKey() : entry.getKey();
    }
}
