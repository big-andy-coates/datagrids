package org.acc.coherence.versioning.temporal;

import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.io.pof.PofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class VValueTest {
    private static final PofContext POF_CONTEXT = new ConfigurablePofContext("org/acc/coherence/versioning/temporal/temporal-versioning-pof-config.xml");

    @Test
    public void shouldSerialise() {
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
        Object actual = VValue.CREATED_POF_EXTRACTED.extract(binary);

        // Then:
        assertThat(actual, is((Object) expected));
    }
}