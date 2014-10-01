package org.acc.coherence.versioning.temporal;

import java.util.*;

/**
 * @author Andy Coates.
 */
public class TimeLine {
    private final Set<Object> allKeys = new HashSet<Object>();
    private final TreeMap<Object, TreeSet<Object>> timeLine = new TreeMap<Object, TreeSet<Object>>();
    private final Comparator comparator;

    public TimeLine() {
        this(null);
    }

    public TimeLine(Comparator comparator) {
        this.comparator = comparator;
    }

    public void insert(Object key, Object timestamp) {
        if (allKeys.contains(key)) {
            throw new UnsupportedOperationException("Duplicate key detected: " + key);
        }

        Set<Object> entries = getEntriesAtTimestamp(timestamp, true);
        entries.add(key);
        allKeys.add(key);
    }

    public boolean remove(Object key, Object timestamp) {
        Set<Object> keys = getEntriesAtTimestamp(timestamp, false);
        if (keys == null || !keys.remove(key)) {
            return false;
        }

        if (keys.isEmpty()) {
            timeLine.remove(timestamp);
        }

        allKeys.remove(key);
        return true;
    }

    public boolean isEmpty() {
        return timeLine.isEmpty();
    }

    public Object get(Object snapshot) {
        Map.Entry<Object, TreeSet<Object>> floor = timeLine.floorEntry(snapshot);
        return floor == null ? null : floor.getValue().last();
    }

    public Collection<Object> keySet() {
        return allKeys;
    }

    private Set<Object> getEntriesAtTimestamp(Object timestamp, boolean createIfNecessary) {
        TreeSet<Object> keys = timeLine.get(timestamp);
        if (keys == null && createIfNecessary) {
            keys = new TreeSet<Object>(comparator);
            timeLine.put(timestamp, keys);
        }
        return keys;
    }
}

