package com.spotify.folsom.client;

import org.junit.Test;

import static org.junit.Assert.*;

public class UtilsTest {
    @Test
    public void testTtlToExpiration() {
        assertEquals(0, Utils.ttlToExpiration(-123123));
        assertEquals(0, Utils.ttlToExpiration(0));
        assertEquals(123, Utils.ttlToExpiration(123));

        assertEquals(2591999, Utils.ttlToExpiration(2591999));

        // Converted to timestamp
        assertTimestamp(2592000);
        assertTimestamp(3000000);

        // Overflows the timestamp
        assertEquals(Integer.MAX_VALUE, Utils.ttlToExpiration(Integer.MAX_VALUE - 1234));
    }

    private void assertTimestamp(int ttl) {
        double expectedTimestamp = (System.currentTimeMillis() / 1000.0) + ttl;
        assertEquals(expectedTimestamp, Utils.ttlToExpiration(ttl), 2.0);
    }
}