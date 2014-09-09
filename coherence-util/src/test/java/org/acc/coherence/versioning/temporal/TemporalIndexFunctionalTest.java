package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.filter.AndFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.IndexAwareFilter;
import com.tangosol.util.filter.NotEqualsFilter;
import org.acc.coherence.test.ClusterBasedTest;
import org.acc.coherence.test.EnsureCacheInvocable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.testng.Assert.fail;

public class TemporalIndexFunctionalTest extends ClusterBasedTest {
    private static final TemporalExtractor TEMPORAL_EXTRACTOR = new TemporalExtractor(
            VKey.BUSINESS_KEY_POF_EXTRACTOR,
            VValue.CREATED_POF_EXTRACTED,
            VKey.VERSION_POF_EXTRACTOR);

    private NamedCache versioned;

    public TemporalIndexFunctionalTest() {
        super("org/acc/coherence/versioning/temporal/temporal-versioning-coherence-cache-config.xml");
    }

    @BeforeMethod
    public void setUp() {
        versioned = reinitialiseCache("versioned-test");
        reinitialiseCache("repl-results");
        EnsureCacheInvocable.ensureCache("repl-results", "InvocationService");
    }

    @Test
    public void shouldAddIndexToEmptyCacheWithoutBlowingUp() {
        // When:
        addTemporalIndex();
    }

    @Test
    public void shouldRemoveIndexFromEmptyCacheWithoutBlowingUp() {
        // Given:
        addTemporalIndex();

        // When:
        removeTemporalIndex();
    }

    @Test
    public void shouldThrowIfNotOrderedIndex() throws Exception {
        try {
            versioned.addIndex(TEMPORAL_EXTRACTOR, false, null);
            fail("Exception expected");
        } catch (Exception e) {
            //assertThat(e.getOriginalException(), instanceOf(IllegalArgumentException.class));
            System.out.println("Exception caught!" + e);
        }
    }

    @Test
    public void shouldAddIndexToCacheWithData() {
        // Given:
        givenSomeDataInCache();

        // When:
        addTemporalIndex();

        // Then:
        assertThat(numberOfIndexes(), is(1));
    }

    @Test
    public void shouldRemoveIndexFromCacheWithData() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();

        // When:
        removeTemporalIndex();

        // Then:
        assertThat(numberOfIndexes(), is(0));
    }

    @Test
    public void shouldBeAbleToQuerySnapShotEqualsFilter_UpperBoundCheck() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();

        // When:
        Set<VKey<String>> results = versioned.keySet(new SnapshotFilter(TEMPORAL_EXTRACTOR, 5000L));

        // Then:
        assertThat(results, containsInAnyOrder(
                new VKey<String>("firstKey", 2),
                new VKey<String>("secondKey", 2),
                new VKey<String>("thirdKey", 1)
        ));
    }

    @Test
    public void shouldBeAbleToQuerySnapShotEqualsFilter_LowerBoundCheck() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();

        // When:
        Set<VKey<String>> results = versioned.keySet(new SnapshotFilter(TEMPORAL_EXTRACTOR, 999L));

        // Then:
        assertThat(results, containsInAnyOrder(
                new VKey<String>("secondKey", 1),
                new VKey<String>("thirdKey", 1)
        ));
    }

    @Test
    public void shouldBeAbleToQueryWithNotMatches() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();

        // When:
        Set<Integer> results = versioned.keySet(new SnapshotFilter(TEMPORAL_EXTRACTOR, 5L));    // No versions existed at this point

        // Then:
        assertThat(results, is(empty()));
    }

    @Test
    public void shouldWorkInConjunctionWithOtherFilters() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();

        // When:
        Set<VKey<String>> results = versioned.keySet(new AndFilter(
                new NotEqualsFilter(VKey.BUSINESS_KEY_POF_EXTRACTOR, "secondKey"),
                new SnapshotFilter(TEMPORAL_EXTRACTOR, 5500L)
        ));

        // Then:
        assertThat(results, containsInAnyOrder(
                new VKey<String>("firstKey", 2),
                new VKey<String>("thirdKey", 2)
        ));
    }

    @Test
    public void shouldReturnHighestVersionWhereTwoVersionsHaveSameTimestamp() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();

        // When:
        Set<VKey<String>> results = versioned.keySet(new AndFilter(
                new EqualsFilter(VKey.BUSINESS_KEY_POF_EXTRACTOR, "thirdKey"),
                new SnapshotFilter(TEMPORAL_EXTRACTOR, 9999L)
        ));

        // Then:
        assertThat(results, containsInAnyOrder(
                new VKey<String>("thirdKey", 4)
        ));
    }

    @Test
    public void shouldSupportEntriesBeingRemoved() throws Exception {
        // Given:
        givenSomeDataInCacheAndIndexExists();
        removeValueFromCache(4, "thirdKey");

        // When:
        Set<VKey<String>> results = versioned.keySet(new AndFilter(
                new EqualsFilter(VKey.BUSINESS_KEY_POF_EXTRACTOR, "thirdKey"),
                new SnapshotFilter(TEMPORAL_EXTRACTOR, 9999L)
        ));

        // Then:
        assertThat(results, containsInAnyOrder(
                new VKey<String>("thirdKey", 3)
        ));
    }

    @Test
    public void shouldBeConsistentAfterPartitionMoves() throws Exception {
        // Given:
        givenPartitionsHaveMoved();

        // Then:
        fail("todo");
    }

    // Todo(ac): test with complex type returned from arrived and with comparator

    // todo(ac): test with non-pof serialistion.

    private void addTemporalIndex() {
        versioned.addIndex(TEMPORAL_EXTRACTOR, true, null);
    }

    private void removeTemporalIndex() {
        versioned.removeIndex(TEMPORAL_EXTRACTOR);
    }

    private NamedCache reinitialiseCache(String name) {
        NamedCache cache = CacheFactory.getCache(name);
        cache.destroy();
        return CacheFactory.getCache(name);
    }

    private void givenSomeDataInCache() {
        // NB: Deliberately initialised out of order to test nothing is dependant on load order
        addValueToCache(1, "firstKey", 1000); // 1000 -> 5000
        addValueToCache(3, "firstKey", 10000); // 10000 -> ?
        addValueToCache(2, "firstKey", 5000);// 5000 -> 10000

        addValueToCache(2, "secondKey", 4999);// 4999 -> 20000
        addValueToCache(3, "secondKey", 20000);// 20000 -> ?
        addValueToCache(1, "secondKey", 500); // 500 -> 5000

        addValueToCache(3, "thirdKey", 9999); // never live, superseded by v4
        addValueToCache(1, "thirdKey", 999); // 999 -> 5001
        addValueToCache(2, "thirdKey", 5001);// 5001 -> 9999
        addValueToCache(4, "thirdKey", 9999); // 9999 -> ?
    }

    private void givenSomeDataInCacheAndIndexExists() {
        givenSomeDataInCache();
        addTemporalIndex();
    }

    private void addValueToCache(int version, String key, long createdTimestamp) {
        VValue<String> value = new VValue<String>("theValue");
        value.setCreated(createdTimestamp);
        value.setVersion(version);
        versioned.put(new VKey<String>(key, version), value);
    }

    private void removeValueFromCache(int version, String key) {
        versioned.remove(new VKey<String>(key, version));
    }

    private int numberOfIndexes() {
        IndexCountingFilter filter = new IndexCountingFilter();
        versioned.aggregate(filter, new Count());
        return filter.getResult();
    }

    private void givenPartitionsHaveMoved() {
    }

    @Portable
    public static class IndexCountingFilter implements IndexAwareFilter {
        @Override
        public int calculateEffectiveness(Map indexMap, Set set) {
            CacheFactory.getCache("repl-results").put("result", indexMap.size());
            return 0;
        }

        @Override
        public Filter applyIndex(Map indexMap, Set set) {
            CacheFactory.getCache("repl-results").put("result", indexMap.size());
            return null;
        }

        @Override
        public boolean evaluateEntry(Map.Entry entry) {
            return false;
        }

        @Override
        public boolean evaluate(Object o) {
            return false;
        }

        public int getResult() {
            return (Integer) CacheFactory.getCache("repl-results").get("result");
        }
    }
}