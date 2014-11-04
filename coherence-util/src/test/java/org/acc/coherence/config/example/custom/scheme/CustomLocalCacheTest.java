package org.acc.coherence.config.example.custom.scheme;

import com.tangosol.net.ExtensibleConfigurableCacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.run.xml.XmlElement;
import com.tangosol.run.xml.XmlHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author jk & acoates 2014.09.24
 */
public class CustomLocalCacheTest {

    private ExtensibleConfigurableCacheFactory ccf;

    @AfterMethod
    public void tearDown() {
        ccf.dispose();
    }

    @Test
    public void shouldCreateCustomLocalCache() throws Exception {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        // When:
        final NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        assertThat(getBackingMap(cache), instanceOf(CustomLocalCache.class));
    }

    @Test
    public void shouldCreateDistributedCacheUsingCustomLocalCache() throws Exception {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-distributed</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "      <distributed-scheme>\n" +
                        "            <scheme-name>custom-distributed</scheme-name>\n" +
                        "            <service-name>DistributedService</service-name>\n" +
                        "            <backing-map-scheme>\n" +
                        "                <acc:custom-local-scheme/>\n" +
                        "            </backing-map-scheme>\n" +
                        "        </distributed-scheme>" +
                        "    </caching-schemes>\n");

        // When:
        NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        assertThat(getBackingMap(cache), instanceOf(CustomLocalCache.class));
    }

    @Test
    public void shouldInjectStandardCacheName() {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "            <acc:example-custom-string-param>Some Value</acc:example-custom-string-param>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        // When:
        final NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getStandardInjectableParam(), is("test-cache"));
    }

    @Test
    public void shouldInjectStandardHighUnits() {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "            <acc:example-custom-string-param>Some Value</acc:example-custom-string-param>\n" +
                        "            <acc:high-units>32000</acc:high-units>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        // When:
        final NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getHighUnits(), is(32000));
    }

    @Test
    public void shouldInjectStandardExpiryDelay() {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "            <acc:example-custom-string-param>Some Value</acc:example-custom-string-param>\n" +
                        "            <acc:expiry-delay>60s</acc:expiry-delay>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        // When:
        final NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getExpiryDelay(), is(60 * 1000));
    }

    @Test
    public void shouldInjectCustomMacroParam() {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "            <acc:example-custom-macro-param>{manager-context}</acc:example-custom-macro-param>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        // When:
        final NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getBackingMapManagerContext(), is(notNullValue()));
    }

    @Test
    public void shouldInjectCustomStringParam() throws Exception {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-distributed</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "      <distributed-scheme>\n" +
                        "            <scheme-name>custom-distributed</scheme-name>\n" +
                        "            <service-name>DistributedService</service-name>\n" +
                        "            <backing-map-scheme>\n" +
                        "                <acc:custom-local-scheme>\n" +
                        "                    <acc:example-custom-string-param>Some Value</acc:example-custom-string-param>\n" +
                        "                </acc:custom-local-scheme>\n" +
                        "            </backing-map-scheme>\n" +
                        "        </distributed-scheme>" +
                        "    </caching-schemes>\n");

        // When:
        NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getExampleCustomStringParam(), is("Some Value"));
    }

    @Test
    public void shouldInjectCustomLongParam() throws Exception {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "            <acc:example-custom-long-param>10</acc:example-custom-long-param>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        // When:
        NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getExampleCustomLongParam(), is(10L));
    }

    @Test
    public void shouldInjectFromResourceRegistry() throws Exception {
        // Given:
        ccf = initialiseCacheFactory(
                "    <caching-scheme-mapping>\n" +
                        "        <cache-mapping>\n" +
                        "            <cache-name>test-cache</cache-name>\n" +
                        "            <scheme-name>custom-local</scheme-name>\n" +
                        "        </cache-mapping>\n" +
                        "    </caching-scheme-mapping>\n" +
                        "    <caching-schemes>\n" +
                        "        <acc:custom-local-scheme>\n" +
                        "            <acc:scheme-name>custom-local</acc:scheme-name>\n" +
                        "            <acc:service-name>CustomLocalService</acc:service-name>\n" +
                        "        </acc:custom-local-scheme>\n" +
                        "    </caching-schemes>\n");

        ccf.getResourceRegistry().registerResource(ExampleResource.class, new ExampleResource());

        // When:
        NamedCache cache = ccf.ensureCache("test-cache", null);

        // Then:
        final CustomLocalCache backingMap = (CustomLocalCache)getBackingMap(cache);
        assertThat(backingMap.getExampleInjectedResource(), is(notNullValue()));
    }

    private static ExtensibleConfigurableCacheFactory initialiseCacheFactory(String configSnippet) {
        final String preFix = "<?xml version=\"1.0\"?>\n" +
                "<cache-config xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                "              xmlns=\"http://xmlns.oracle.com/coherence/coherence-cache-config\"\n" +
                "              xmlns:acc=\"class://org.acc.coherence.config.example.custom.scheme.CustomNamespaceHandler\">\n";

        final String postFix = "</cache-config>";

        final XmlElement xmlConfig = XmlHelper.loadXml(preFix + configSnippet + postFix);

        ExtensibleConfigurableCacheFactory cacheFactory = new ExtensibleConfigurableCacheFactory(ExtensibleConfigurableCacheFactory.DependenciesHelper.newInstance(xmlConfig));
        cacheFactory.setScopeName("CustomLocalCacheTest");
        return cacheFactory;
    }

    private static Map getBackingMap(NamedCache cache) {
        final ExtensibleConfigurableCacheFactory.Manager manager =
                (ExtensibleConfigurableCacheFactory.Manager) cache.getCacheService().getBackingMapManager();
        return manager.getBackingMap(cache.getCacheName());
    }
}
