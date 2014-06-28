package org.acc.coherence.versioning.temporal;

import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.*;
import com.tangosol.util.extractor.PofExtractor;
import org.apache.commons.lang3.Validate;

import java.util.*;

/**
 * Custom index capable on indexing synthesised fields e.g. base on other map entries.
 */
public class SyntheticIndex implements MapIndex {
    private final ValueExtractor indexExtractor;
    private final ValueExtractor actualExtractor;
    private final EntrySequencer entrySequencer;
    private final boolean ordered;
    private final Comparator comparator;
    private final BackingMapContext context;
    private final Map<Object, Set<Object>> index;

    public SyntheticIndex(ValueExtractor indexExtractor, ValueExtractor actualExtractor, EntrySequencer entrySequencer,
                          boolean ordered, Comparator<Object> comparator, BackingMapContext context) {
        Validate.notNull(indexExtractor);
        Validate.notNull(actualExtractor);
        Validate.notNull(entrySequencer);
        Validate.notNull(context);

        this.indexExtractor = indexExtractor;
        this.actualExtractor = actualExtractor;
        this.entrySequencer = entrySequencer;
        this.ordered = ordered;
        this.comparator = comparator;
        this.context = context;
        this.index = ordered ? new TreeMap<Object, Set<Object>>(comparator) : new HashMap<Object, Set<Object>>();
    }

    @Override
    public ValueExtractor getValueExtractor() {
        return indexExtractor;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public boolean isPartial() {
        return true;    // i.e. not all entries are in index.
    }

    @Override
    public Map getIndexContents() {
        return index;
    }

    @Override
    public Object get(Object key) {
        Object result = index.get(key);
        return result == null ? NO_VALUE : result;
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

        Set<Object> keys = index.get(value);
        if (keys == null) {
            keys = new HashSet<Object>();
            index.put(value, keys);
        }

        keys.add(key);
    }

    private void delete(Object key, Object value) {
        if (key == null || value == null) {
            return;
        }

        Set<Object> keys = index.get(value);
        if (keys == null) {
            return;
        }

        if (!keys.remove(key)) {
            return;
        }

        if (keys.isEmpty()) {
            index.remove(value);
        }
    }

    private Object extractIndexedAttribute(final Map.Entry entry) {
        if (entry == null) {
            return null;
        }

        if (!(actualExtractor instanceof PofExtractor) || (entry instanceof BinaryEntry)) {
            return InvocableMapHelper.extractFromEntry(actualExtractor, entry);
        }
        BinaryEntry binaryEntry = new BinaryEntry() {
            @Override
            public Binary getBinaryKey() {
                return (Binary) entry.getKey();
            }

            @Override
            public Binary getBinaryValue() {
                return (Binary) entry.getValue();
            }

            @Override
            public Serializer getSerializer() {
                return context.getManagerContext().getCacheService().getSerializer();
            }

            @Override
            public BackingMapManagerContext getContext() {
                return context.getManagerContext();
            }

            @Override
            public void updateBinaryValue(Binary binary) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void updateBinaryValue(Binary binary, boolean b) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getOriginalValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Binary getOriginalBinaryValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ObservableMap getBackingMap() {
                throw new UnsupportedOperationException();
            }

            @Override
            public BackingMapContext getBackingMapContext() {
                return context;
            }

            @Override
            public void expire(long l) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getExpiry() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isReadOnly() {
                return true;
            }

            @Override
            public void setValue(Object o, boolean b) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void remove(boolean b) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getKey() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object setValue(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void update(ValueUpdater valueUpdater, Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isPresent() {
                return true;
            }

            @Override
            public Object extract(ValueExtractor valueExtractor) {
                throw new UnsupportedOperationException();
            }
        };
        return InvocableMapHelper.extractFromEntry(actualExtractor, binaryEntry);
    }

    private static Object getKey(Map.Entry entry) {
        if (entry == null) {
            return null;
        }

        return entry instanceof BinaryEntry ? ((BinaryEntry) entry).getBinaryKey() : entry.getKey();
    }
}
