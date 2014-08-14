package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Wrapper type for versioned values.
 */
@Portable
public class VValue<DomainValue> implements Versioned<DomainValue> {
    public static final int METADATA_POF_ID = 1;
    public static final PofExtractor VERSION_POF_EXTRACTOR = new PofExtractor(int.class, new SimplePofPath(new int[]{METADATA_POF_ID, MetaData.VERSION_POF_ID}));
    public static final PofExtractor CREATED_POF_EXTRACTED = new PofExtractor(long.class, new SimplePofPath(new int[]{METADATA_POF_ID, MetaData.CREATED_POF_ID}));

    @PortableProperty(value = METADATA_POF_ID)
    private MetaData metaData = new MetaData();

    @PortableProperty(value = DOMAIN_POF_ID)
    private DomainValue domainValue;

    @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
    @Deprecated                             // Only
    public VValue() {
    }

    public VValue(DomainValue value) {
        domainValue = value;
    }

    @Override
    public int getVersion() {
        return metaData.getVersion();
    }

    @Override
    public void setVersion(int version) {
        metaData.setVersion(version);
    }

    @Override
    public DomainValue getDomainObject() {
        return domainValue;
    }

    public void setCreated(long timestamp) {
        metaData.setCreated(timestamp);
    }

    public long getCreated() {
        return metaData.getCreated();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VValue that = (VValue) o;
        if (domainValue != null ? !domainValue.equals(that.domainValue) : that.domainValue != null) return false;
        if (!metaData.equals(that.metaData)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * metaData.hashCode() + (domainValue != null ? domainValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "VValue{metaData=" + metaData + ", domainValue=" + domainValue + '}';
    }

    @Portable
    public static class MetaData { // Todo(ac): make more generic
        public final static int VERSION_POF_ID = 1;
        public final static int CREATED_POF_ID = 2;
        private static final int NOT_SET = -1;

        @PortableProperty(value = VERSION_POF_ID)
        private int version = NOT_SET;

        @PortableProperty(value = CREATED_POF_ID)
        private long created = NOT_SET;

        public MetaData() {
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public void setCreated(long timestamp) {
            created = timestamp;
        }

        public long getCreated() {
            return created;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetaData that = (MetaData) o;
            if (created != that.created) return false;
            if (version != that.version) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 31 * version + (int) (created ^ (created >>> 32));
        }

        @Override
        public String toString() {
            return "MetaData{version=" + version + ", created=" + created + '}';
        }
    }
}