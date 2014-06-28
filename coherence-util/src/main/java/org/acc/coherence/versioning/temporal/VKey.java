package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Wrapper type for versioned keys
 */
@Portable
public class VKey<DomainKey> implements Versioned<DomainKey> {
    public static final int FIRST_VERSION = 1;
    public static final int VERSION_POF_ID = 1;
    public static final PofExtractor VERSION_POF_EXTRACTED = new PofExtractor(int.class, new SimplePofPath(VERSION_POF_ID), PofExtractor.KEY);

    @PortableProperty(value = VERSION_POF_ID)
    private int version;

    @PortableProperty(value = DOMAIN_POF_ID)
    private DomainKey domainKey;

    @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
    @Deprecated                             // Only
    public VKey() {
    }

    public VKey(DomainKey domainKey, int version) {
        this.domainKey = domainKey;
        this.version = version;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public DomainKey getDomainObject() {
        return domainKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VKey that = (VKey) o;
        if (version != that.version) return false;
        if (domainKey != null ? !domainKey.equals(that.domainKey) : that.domainKey != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * version + (domainKey != null ? domainKey.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "VKey{version=" + version + ", domainKey=" + domainKey + '}';
    }
}