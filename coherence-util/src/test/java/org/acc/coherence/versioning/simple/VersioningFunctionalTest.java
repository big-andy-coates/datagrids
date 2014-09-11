package org.acc.coherence.versioning.simple;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import org.acc.coherence.test.ClusterBasedTest;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class VersioningFunctionalTest extends ClusterBasedTest {
    public VersioningFunctionalTest() {
        super("org/acc/coherence/versioning/simple/simple-versioning-coherence-cache-config.xml");
    }

    @Test(enabled = false)
    public void shouldDoSomething() {
        NamedCache cache = CacheFactory.getCache("dist-latest");
        assertThat(cache, is(notNullValue()));

        cache.put("someKey", "version 1 of value");
    }
}