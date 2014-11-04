package org.acc.coherence.index;

import com.sun.org.apache.xml.internal.utils.WrappedRuntimeException;
import com.tangosol.io.pof.PortableException;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.WrapperException;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.ReflectionExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.LessFilter;
import org.acc.coherence.test.ClusterBasedTest;
import org.apache.commons.lang3.Validate;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testng.Assert.*;

public class IndexContentExtractorFunctionalTest extends ClusterBasedTest {

    public IndexContentExtractorFunctionalTest() {
        super("org/acc/coherence/index/index-coherence-cache-config.xml");
    }

    @Test
    public void shouldThrowIfNoIndexMatchingExtractor() {
        // Given:
        NamedCache cache = givenSomeDataInCache("pof-distributed-cache-test", 7);
        PofExtractor unknownExtractor = new PofExtractor(null, 101);

        try {
            // When:
            cache.aggregate(AlwaysFilter.INSTANCE, new IndexContentExtractor(unknownExtractor));
        } catch (PortableException e){
            assertThat(e.getCause(), instanceOf(PortableException.class));
            assertThat(((PortableException)e.getCause()).getName(), containsString(IllegalStateException.class.getCanonicalName()));
        }
    }

    @Test
    public void shouldThrowExceptionIfNotBinaryCache() {
        // Given:
        NamedCache cache = CacheFactory.getCache("replicated-cache-test");

        try {
            // When:
            cache.aggregate(AlwaysFilter.INSTANCE, new IndexContentExtractor(TestType.INTEGER_PROP_JAVA_EXTRACTOR));
        } catch (PortableException e){
            assertThat(e.getCause(), instanceOf(PortableException.class));
            assertThat(((PortableException)e.getCause()).getName(), containsString(IllegalArgumentException.class.getCanonicalName()));
        }
    }

    @Test
    public void shouldExtractIndexContentFromPofBinaryCaches() {
        // Given:
        NamedCache cache = givenSomeDataAndIndexesInCache("pof-distributed-cache-test", 7);

        // When:
        Object result = cache.aggregate(AlwaysFilter.INSTANCE, new IndexContentExtractor(TestType.INTEGER_PROP_POF_EXTRACTOR));

        // Then:
        assertThat(result, is(instanceOf(Set.class)));

        Set<Integer> results = (Set<Integer>)result;
        assertThat(results, containsInAnyOrder(0, 1, 2, 3, 4));
    }

    @Test
    public void shouldExtractIndexContentFromJavaBinaryCaches() {
        // Given:
        NamedCache cache = givenSomeDataAndIndexesInCache("java-distributed-cache-test", 7);

        // When:
        Object result = cache.aggregate(AlwaysFilter.INSTANCE, new IndexContentExtractor(TestType.STRING_PROP_JAVA_EXTRACTOR));

        // Then:
        assertThat(result, is(instanceOf(Set.class)));

        Set<Integer> results = (Set<Integer>)result;
        assertThat(results, containsInAnyOrder("0", "1", "2", "3", "4"));
    }

    @Test
    public void shouldExtractOnlyContentThatMatchesFilter() {
        // Given:
        NamedCache cache = givenSomeDataAndIndexesInCache("pof-distributed-cache-test", 30);
        Filter filter = new LessFilter(IdentityExtractor.INSTANCE, 3);

        // When:
        Object result = cache.aggregate(AlwaysFilter.INSTANCE, new IndexContentExtractor(TestType.INTEGER_PROP_POF_EXTRACTOR, filter));

        // Then:
        assertThat(result, is(instanceOf(Set.class)));

        Set<Integer> results = (Set<Integer>)result;
        assertThat(results, containsInAnyOrder(0, 1, 2));
    }

    private static NamedCache givenSomeDataInCache(String cacheName, int count) {
        NamedCache cache = CacheFactory.getCache(cacheName);

        for (int key = 0; key != count; ++key) {
            int value = key % 5;
            cache.put(key, new TestType("" + value, value));
        }

        return cache;
    }

    private static NamedCache givenSomeDataAndIndexesInCache(String cacheName, int count) {
        NamedCache cache = givenSomeDataInCache(cacheName, count);
        if (cacheName.contains("java")) {
            cache.addIndex(TestType.INTEGER_PROP_JAVA_EXTRACTOR, true, null);
            cache.addIndex(TestType.STRING_PROP_JAVA_EXTRACTOR, false, null);
        } else {
            cache.addIndex(TestType.INTEGER_PROP_POF_EXTRACTOR, true, null);
            cache.addIndex(TestType.STRING_PROP_POF_EXTRACTOR, false, null);
        }
        return cache;
    }

    @Portable
    public static class TestType implements Serializable {
        private static final long serialVersionUID = 5535719526505964165L;

        public static final ValueExtractor STRING_PROP_POF_EXTRACTOR = new PofExtractor(String.class, 1);
        public static final ValueExtractor STRING_PROP_JAVA_EXTRACTOR = new ReflectionExtractor("getStringProp");
        public static final ValueExtractor INTEGER_PROP_POF_EXTRACTOR = new PofExtractor(Integer.class, 2);
        public static final ValueExtractor INTEGER_PROP_JAVA_EXTRACTOR = new ReflectionExtractor("getIntegerProp");

        @PortableProperty(value = 1)
        private String stringProp;

        @PortableProperty(value = 2)
        private int integerProp;

        @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
        @Deprecated                             // Only
        public TestType() {
        }

        public TestType(String s, int i) {
            Validate.notNull(s, "string value can not be null");
            this.stringProp = s;
            this.integerProp = i;
        }

        @SuppressWarnings("UnusedDeclaration")      // Called via reflection
        public String getStringProp() {
            return stringProp;
        }

        @SuppressWarnings("UnusedDeclaration")      // Called via reflection
        public int getIntegerProp() {
            return integerProp;
        }
    }
}