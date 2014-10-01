package org.acc.coherence.config.example.custom.scheme;

import com.tangosol.config.annotation.Injectable;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.cache.LocalCache;

/**
 * @author jk & acoates 2014.09.24
 */
public class CustomLocalCache extends LocalCache {

    private final String exampleCustomStringParam;
    private final Long exampleCustomLongParam;
    private final BackingMapManagerContext ctx;

    private String standardInjectableParam;
    private ExampleResource exampleInjectedResource;

    public CustomLocalCache(String exampleCustomStringParam, Long exampleCustomLongParam, BackingMapManagerContext ctx) {
        this.exampleCustomStringParam = exampleCustomStringParam;
        this.exampleCustomLongParam = exampleCustomLongParam;
        this.ctx = ctx;
    }

    /**
     * Add support for the standard local-scheme's 'high-unit' element, which would otherwise not be set as LocalCache does not use @Injectable
     * @param highUnits the high units
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable(value = "high-units")
    public void configureHighUnits(int highUnits) {
        super.setHighUnits(highUnits);
    }

    /**
     * Add support for the standard local-scheme's 'expiry-delay' element, which would otherwise not be set as LocalCache does not use @Injectable
     * @param delayMs the delay in milliseconds
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable(value = "expiry-delay")
    public void configureExpiryDelay(int delayMs) {
        super.setExpiryDelay(delayMs);
    }

    /**
     * An example of how anything from the resource registry can also be captured via an injectable.
     * @param example the item fromm the resource registry.
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable
    public void setExampleResource(ExampleResource example) {
         this.exampleInjectedResource = example;
    }

    /**
     * An example of how to capture a standard injectable i.e. the cache name.
     * Supported standard injectables as of Coherence 12.1.2 are 'cache-name', 'class-loader' and 'manager-context'
     * @param cacheName the injected cache name
     */
    @SuppressWarnings("UnusedDeclaration")  // Called by Coherence
    @Injectable(value = "cache-name")
    public void setStandardInjectableParam(String cacheName) {
        this.standardInjectableParam = cacheName;
    }

    public String getStandardInjectableParam() {
        return standardInjectableParam;
    }

    public String getExampleCustomStringParam() {
        return exampleCustomStringParam;
    }

    public Long getExampleCustomLongParam() {
        return exampleCustomLongParam;
    }

    public ExampleResource getExampleInjectedResource() {
        return exampleInjectedResource;
    }

    public BackingMapManagerContext getBackingMapManagerContext() {
        return ctx;
    }
}
