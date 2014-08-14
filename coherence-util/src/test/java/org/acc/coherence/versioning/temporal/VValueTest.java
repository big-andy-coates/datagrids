package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.io.pof.PofContext;
import com.tangosol.io.pof.reflect.PofValue;
import com.tangosol.io.pof.reflect.PofValueParser;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class VValueTest {
    private static final PofContext POF_CONTEXT = new ConfigurablePofContext("org/acc/coherence/versioning/temporal/temporal-versioning-pof-config.xml");

    @Test
    public void shouldReturnDomainObject() throws Exception {
        // When:
        VValue<Long> value = new VValue<Long>(10L);

        // Then:
        assertThat(value.getDomainObject(), is(10L));
    }

    @Test
    public void shouldReturnVersion() throws Exception {
        // Given:
        VValue<Long> value = new VValue<Long>(10L);

        // When:
        value.setVersion(99);

        // Then:
        assertThat(value.getVersion(), is(99));
    }

    @Test
    public void shouldReturnCreatedTimestamp() throws Exception {
        // Given:
        VValue<Long> value = new VValue<Long>(10L);

        // When:
        value.setCreated(123);

        // Then:
        assertThat(value.getCreated(), is(123L));
    }

    @Test
    public void shouldSerialiseAndDeserialiseAllAttributes() {
        // Given:
        final VValue<String> expected = new VValue<String>("DomainValue");
        expected.setVersion(123);
        expected.setCreated(System.currentTimeMillis());

        // When:
        Binary binary = ExternalizableHelper.toBinary(expected, POF_CONTEXT);
        VValue<String> actual = (VValue<String>) ExternalizableHelper.fromBinary(binary, POF_CONTEXT);

        // Then:
        assertThat(expected, is(actual));
    }

    @Test
    public void shouldExtractCreatedTimestamp() {
        // Given:
        final long expected = System.currentTimeMillis();
        final VValue<String> value = new VValue<String>("DomainValue");
        value.setCreated(expected);
        Binary binary = ExternalizableHelper.toBinary(value, POF_CONTEXT);

        // When:
        PofValue pofValue = PofValueParser.parse(binary, POF_CONTEXT);
        Object actual = VValue.CREATED_POF_EXTRACTED.getNavigator().navigate(pofValue).getValue();

        // Then:
        assertThat(actual, is((Object) expected));
    }

    @Test
    public void shouldExtractVersion() {
        // Given:
        final int expected = 101;
        final VValue<String> value = new VValue<String>("DomainValue");
        value.setVersion(expected);
        Binary binary = ExternalizableHelper.toBinary(value, POF_CONTEXT);

        // When:
        PofValue pofValue = PofValueParser.parse(binary, POF_CONTEXT);
        Object actual = VValue.VERSION_POF_EXTRACTOR.getNavigator().navigate(pofValue).getValue();

        // Then:
        assertThat(actual, is((Object) expected));
    }
}