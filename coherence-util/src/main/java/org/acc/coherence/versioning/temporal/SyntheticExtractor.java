package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.ValueExtractor;
import org.apache.commons.lang3.Validate;

/**
 * Created by andy on 01/07/2014.
 */
@Portable
public class SyntheticExtractor implements ValueExtractor {
    @PortableProperty(value = 1)
    private String name;

    @SuppressWarnings("UnusedDeclaration")  // Used by Coherence
    @Deprecated                             // Only
    public SyntheticExtractor() {
    }

    public SyntheticExtractor(String name) {
        Validate.notEmpty(name);
        this.name = name;
    }

    @Override
    public Object extract(Object o) {
        throw new UnsupportedOperationException("SyntheticExtractors don't support actual extraction - only filter and aggregation: " + this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyntheticExtractor that = (SyntheticExtractor) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "SyntheticExtractor{name='" + name + "\'}";
    }
}
