package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.GreaterEqualsFilter;
import com.tangosol.util.filter.GreaterFilter;
import com.tangosol.util.filter.IndexAwareFilter;
import org.acc.coherence.test.ClusterBasedTest;
import org.acc.coherence.test.EnsureCacheInvocable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.testng.Assert.fail;

public class SyntheticIndexFunctionalTest extends ClusterBasedTest {
    private NamedCache versioned;

    public SyntheticIndexFunctionalTest() {
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
        addMetaDataIndex();
    }

    @Test
    public void shouldRemoveIndexFromEmptyCacheWithoutBlowingUp() {
        // Given:
        addMetaDataIndex();

        // When:
        versioned.removeIndex(VValue.CUSTOM_SUPERSEDED_EXTRACTOR);
    }

    @Test
    public void shouldAddIndexToCacheWithData() {
        // Given:
        givenSomeDataInCache();

        // When:
        addMetaDataIndex();

        // Then:
        assertThat(numberOfIndexes(), is(1));
    }

    @Test
    public void shouldRemoveIndexFromCacheWithData() throws Exception {
        // Given:
        givenSomeDataInCache();
        addMetaDataIndex();

        // When:
        versioned.removeIndex(VValue.CUSTOM_SUPERSEDED_EXTRACTOR);

        // Then:
        assertThat(numberOfIndexes(), is(0));
    }

    @Test
    public void shouldBeAbleToQueryByEquals() throws Exception {
        // Given:
        addMetaDataIndex();
        givenSomeDataInCache();

        // When:
        Set<VKey<String>> results = versioned.keySet(new EqualsFilter(VValue.SUPERSEDED_POF_EXTRACTED, 5000L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("secondKey", 1)));
    }

    @Test
    public void shouldBeAbleToQueryWithNotMatches() throws Exception {
        // Given:
        givenSomeDataInCache();
        addMetaDataIndex();

        // When:
        Set<Integer> results = versioned.keySet(new EqualsFilter(VValue.CREATED_POF_EXTRACTED, 123L));

        // Then:
        assertThat(results, is(empty()));
    }

    @Test
    public void shouldBeAbleToQueryByGreaterThan() throws Exception {
        // Given:
        givenSomeDataInCache();
        addMetaDataIndex();

        // When:
        Set<VKey<String>> results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 0L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("firstKey", 2), new VKey<String>("secondKey", 1)));

        // When:
        results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 2500L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("firstKey", 2), new VKey<String>("secondKey", 1)));

        // When:
        results = versioned.keySet(new GreaterEqualsFilter(VValue.SUPERSEDED_POF_EXTRACTED, 5000L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("firstKey", 2), new VKey<String>("secondKey", 1)));

        // When:
        results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 5000L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 2)));

        // When:
        results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 10100L));

        // Then:
        assertThat(results, hasSize(0));
    }

    @Test
    public void shouldStillWorkIfLoadedOutOfOrder() throws Exception {
        // Given:
        addMetaDataIndex();

        addValueToCache(2, "firstKey", 5000);// 5000 -> 10000
        addValueToCache(3, "firstKey", 10000); // 10000 -> ?
        addValueToCache(1, "firstKey", 1000); // 1000 -> 5000

        addValueToCache(1, "secondKey", 500); // 500 -> 5000
        addValueToCache(2, "secondKey", 5000);// 5000 -> ?

        // When:
        Set<VKey<String>> results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 0L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("firstKey", 2), new VKey<String>("secondKey", 1)));

        // When:
        results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 2500L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("firstKey", 2), new VKey<String>("secondKey", 1)));

        // When:
        results = versioned.keySet(new GreaterEqualsFilter(VValue.SUPERSEDED_POF_EXTRACTED, 5000L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 1), new VKey<String>("firstKey", 2), new VKey<String>("secondKey", 1)));

        // When:
        results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 5000L));

        // Then:
        assertThat(results, containsInAnyOrder(new VKey<String>("firstKey", 2)));

        // When:
        results = versioned.keySet(new GreaterFilter(VValue.SUPERSEDED_POF_EXTRACTED, 10100L));

        // Then:
        assertThat(results, hasSize(0));
    }

    @Test
    public void shouldWorkAfterPartitionMoves() throws Exception {
        fail("Todo(ac)");
    }

    private void addMetaDataIndex() {
        versioned.addIndex(VValue.CUSTOM_SUPERSEDED_EXTRACTOR, true, null);
    }

    private NamedCache reinitialiseCache(String name) {
        NamedCache cache = CacheFactory.getCache(name);
        cache.destroy();
        return CacheFactory.getCache(name);
    }

    private void givenSomeDataInCache() {
        addValueToCache(1, "firstKey", 1000); // 1000 -> 5000
        addValueToCache(2, "firstKey", 5000);// 5000 -> 10000
        addValueToCache(3, "firstKey", 10000); // 10000 -> ?

        addValueToCache(1, "secondKey", 500); // 500 -> 5000
        addValueToCache(2, "secondKey", 5000);// 5000 -> ?
    }

    private void addValueToCache(int version, String key, long createdTimestamp) {
        VValue<String> value = new VValue<String>("theValue");
        value.setCreated(createdTimestamp);
        value.setVersion(version);
        versioned.put(new VKey<String>(key, version), value);
    }

    private int numberOfIndexes() {
        IndexCountingFilter filter = new IndexCountingFilter();
        versioned.aggregate(filter, new Count());
        return filter.getResult();
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

    ;
}