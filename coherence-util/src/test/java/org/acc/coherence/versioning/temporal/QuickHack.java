package org.acc.coherence.versioning.temporal;

import java.util.*;

/**
 * Created by andy on 08/08/2014.
 */
public class QuickHack {
    private final static int NUM_KEYS = 100;
    private final static int NUM_UPDATES = NUM_KEYS * 1000;
    private final static int MAX_VERSION = 1000;
    private final static int QUERY_ITERATIONS = 1000;
    private final static Random rng = new Random();

    private final List<VersionedKey> updates;
    private final long maxTimestamp;

    public QuickHack() {
        updates = generateUpdateSequence();
        maxTimestamp = getMaxTimestamp(updates);
    }

    public static void main(String[] args) {
        new QuickHack().run();
    }

    private void run() {

        // Warm up:
        runPerfTest(new V1(), false);
        runPerfTest(new V2(), false);

        // Actual:
        runPerfTest(new V1(), true);
        runPerfTest(new V2(), true);
    }

    private static List<VersionedKey> generateUpdateSequence() {
        final List<String> keys = generateKeys();
        final List<VersionedKey> updates = new ArrayList<VersionedKey>(NUM_UPDATES);

        for (int i = 0; i != NUM_UPDATES; ++i) {
            final String key = pickRandomKey(keys);
            final int version = randomVersion();
            updates.add(new VersionedKey(key, version));
        }

        return Collections.unmodifiableList(updates);
    }

    private static long getMaxTimestamp(List<VersionedKey> updates) {
        long max = 0;
        for (VersionedKey update : updates) {
            max = Math.max(max, update.timestamp);
        }
        return max;
    }

    private void runPerfTest(TestCase testCase, boolean log) {
        final long popTook = populateTestCase(testCase);
        final long queryTook = perfTestQuery(testCase);

        if (log) {
            System.out.println("Test case: " + testCase.getClass().getSimpleName() + ". Population took " + popTook + "ms");
            System.out.println("Test case: " + testCase.getClass().getSimpleName() + ". Query took " + queryTook + "ms");
            System.out.println("Test case: " + testCase.getClass().getSimpleName() + ". Total took " + (queryTook + popTook) + "ms");
        }
    }

    private long populateTestCase(TestCase testCase) {
        final long start = System.currentTimeMillis();

        // Populate:
        Set<VersionedKey> seen = new HashSet<VersionedKey>();
        for (VersionedKey update : updates) {
            if (seen.add(update)) {
                testCase.addValue(update);
            } else {
                seen.remove(update);
                testCase.removeValue(update);
            }
        }

        return System.currentTimeMillis() - start;
    }

    private long perfTestQuery(TestCase testCase) {
        final long start = System.currentTimeMillis();

        final long step = maxTimestamp / QUERY_ITERATIONS;

        for (int i = 0; i != QUERY_ITERATIONS; ++i) {
            queryTestCase(testCase, i * step);
        }

        return System.currentTimeMillis() - start;
    }

    private void queryTestCase(TestCase testCase, long snapshot) {
        testCase.queryAll(snapshot);
    }

    private interface TestCase {

        void addValue(VersionedKey key);

        void removeValue(VersionedKey key);

        Map<String, Integer> queryAll(long snapshot);
    }

    private static class V1 implements TestCase {
        final Map<VersionedKey, Long> arrivedForward = new HashMap<VersionedKey, Long>();
        final TreeMap<Long, Set<VersionedKey>> arrivedReverse = new TreeMap<Long, Set<VersionedKey>>((Comparator<Long>) null);
        final TreeMap<Long, Set<VersionedKey>> supersededReverse = new TreeMap<Long, Set<VersionedKey>>((Comparator<Long>) null);

        @Override
        public void addValue(VersionedKey key) {
            Set<VersionedKey> versions = arrivedReverse.get(key.timestamp);
            if (versions == null) {
                versions = new HashSet<VersionedKey>();
                arrivedReverse.put(key.timestamp, versions);
            }
            versions.add(key);

            arrivedForward.put(key, key.timestamp);

            //Long previousVersion = arrivedForward.get(new VersionedKey(key.key, ))
        }

        @Override
        public void removeValue(VersionedKey key) {
            Set<VersionedKey> versions = arrivedReverse.get(key.timestamp);
            versions.remove(key);
            if (versions.isEmpty()) {
                arrivedReverse.remove(key.timestamp);
            }

            arrivedForward.remove(key);
        }

        @Override
        public Map<String, Integer> queryAll(long snapshot) {
            //arrivedReverse.tailMap()
            return null;
        }
    }

    private static class V2 implements TestCase {
        final Map<String, TreeMap<Long, Integer>> timeLineIndex = new HashMap<String, TreeMap<Long, Integer>>();

        @Override
        public void addValue(VersionedKey key) {
            TreeMap<Long, Integer> timeline = timeLineIndex.get(key.key);
            if (timeline == null) {
                timeline = new TreeMap<Long, Integer>();
                timeLineIndex.put(key.key, timeline);
            }

            timeline.put(key.timestamp, key.version);
        }

        @Override
        public void removeValue(VersionedKey key) {
            TreeMap<Long, Integer> timeline = timeLineIndex.get(key.key);
            timeline.remove(key.timestamp);
            if (timeline.isEmpty()) {
                timeLineIndex.remove(key.key);
            }
        }

        @Override
        public Map<String, Integer> queryAll(long snapshot) {
            final Map<String, Integer> results = new HashMap<String, Integer>();
            for (Map.Entry<String, TreeMap<Long, Integer>> businessEntry : timeLineIndex.entrySet()) {
                String key = businessEntry.getKey();
                TreeMap<Long, Integer> timeline = businessEntry.getValue();

                Map.Entry<Long, Integer> possibleEntry = timeline.floorEntry(snapshot);
                if (possibleEntry != null) {
                    results.put(key, possibleEntry.getValue());
                }
            }
            return results;
        }
    }

    private static class VersionedKey {
        public final String key;
        public final int version;
        public final long timestamp;

        public VersionedKey(String key, int version) {
            this.key = key;
            this.version = version;
            this.timestamp = version * key.length();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VersionedKey update = (VersionedKey) o;

            if (version != update.version) return false;
            if (key != null ? !key.equals(update.key) : update.key != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + version;
            return result;
        }

        @Override
        public String toString() {
            return '[' + key + " = " + version + ']';
        }
    }

    private static List<String> generateKeys() {
        final List<String> keys = new ArrayList<String>(NUM_KEYS);
        for (int i = 0; i != NUM_KEYS; ++i) {
            keys.add("" + i);
        }
        return Collections.unmodifiableList(keys);
    }

    private static String pickRandomKey(List<String> keys) {
        return keys.get(rng.nextInt(keys.size()));
    }

    private static int randomVersion() {
        return rng.nextInt(MAX_VERSION) + 1;
    }
}
