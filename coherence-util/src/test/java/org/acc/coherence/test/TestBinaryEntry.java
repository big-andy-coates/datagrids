package org.acc.coherence.test;

import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.*;

/**
 * Created 10/09/2014
 *
 * @author Andy Coates.
 */
public class TestBinaryEntry implements BinaryEntry, MapTrigger.Entry {
    private final Object key;
    private final Object value;
    private final Object originalValue;
    private final Serializer serialiser;

    public TestBinaryEntry(Object key, Object value, Serializer serialiser) {
        this(key, value, null, serialiser);
    }

    public TestBinaryEntry(Object key, Object value, Object originalValue, Serializer serialiser) {
        this.key = key;
        this.value = value;
        this.originalValue = originalValue;
        this.serialiser = serialiser;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Object getOriginalValue() {
        return originalValue;
    }

    @Override
    public boolean isOriginalPresent() {
        return originalValue != null;
    }

    @Override
    public Binary getBinaryKey() {
        return ExternalizableHelper.toBinary(key, serialiser);
    }

    @Override
    public Binary getBinaryValue() {
        return ExternalizableHelper.toBinary(value, serialiser);
    }

    @Override
    public Binary getOriginalBinaryValue() {
        return ExternalizableHelper.toBinary(originalValue, serialiser);
    }

    @Override
    public Serializer getSerializer() {
        return serialiser;
    }

    @Override
    public BackingMapManagerContext getContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryValue(Binary binary) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryValue(Binary binary, boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObservableMap getBackingMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BackingMapContext getBackingMapContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void expire(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getExpiry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object setValue(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValue(Object o, boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(ValueUpdater valueUpdater, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPresent() {
        return value != null;
    }

    @Override
    public void remove(boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object extract(ValueExtractor valueExtractor) {
        return InvocableMapHelper.extractFromEntry(valueExtractor, this);
    }

    @Override
    public String toString() {
        return "TestBinaryEntry{" +
                "key=" + key +
                ", value=" + value +
                ", originalValue=" + originalValue +
                ", serialiser=" + serialiser +
                '}';
    }
}
