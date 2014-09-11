package org.acc.coherence.test;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.AbstractInvocable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.InvocationService;

import java.io.Serializable;

/**
 * Ensure a cache is available on each node - useful for ensure replicated caches have been instantiated on all nodes.
 */
@Portable
public class EnsureCacheInvocable extends AbstractInvocable implements Serializable {
    private static final long serialVersionUID = -3234147557166220459L;

    public static void ensureCache(String cacheName, String invocationService) {
        InvocationService service = (InvocationService) CacheFactory.getService(invocationService);
        service.query(new EnsureCacheInvocable(cacheName), null);
    }

    @PortableProperty(value = 1)
    private String cacheName;

    @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
    @Deprecated                             // Only
    public EnsureCacheInvocable() {

    }

    public EnsureCacheInvocable(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public void run() {
        CacheFactory.getCache(cacheName);
    }
}
