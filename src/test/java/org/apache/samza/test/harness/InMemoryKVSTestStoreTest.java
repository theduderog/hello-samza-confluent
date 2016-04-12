package org.apache.samza.test.harness;

import org.apache.avro.generic.GenericData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

//mostly copy from org.apache.samza.storage.kv.TestKeyValueStores
public class InMemoryKVSTestStoreTest {

    InMemoryKVTestStore<String, String> store;

    @Before
    public void setup() {
        store = new InMemoryKVTestStore<String, String>();
    }

    @After
    public void teardown() {
        store.close();
    }

    @Test
    public void getNonExistentIsNull() {
        assert (store.get("hello") == null);
    }

    @Test
    public void testGetAllWhenZeroMatch() {
        store.put("hello", "world");
        List<String> keys = new ArrayList<String>();
        keys.add("foo");
        keys.add("bar");
        Map<String, String> actual = store.getAll(keys);

        for (String k : keys) {
            assert (actual.get(k) == null);
        }
    }

    @Test
    public void testGetAllWhenFullMatch() {
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("k0", "v0");
        expected.put("k1", "v1");

        store.put("k0", "v0");
        store.put("k1", "v1");
        Map<String, String> actual = store.getAll(new ArrayList<String>(expected.keySet()));

        assert (expected.size() == actual.size());

        for (String k : expected.keySet()) {
            assert (expected.get(k).equals(actual.get(k)));
        }
    }

    @Test
    public void testGetAllWhenPartialMatch() {
        store.put("k0", "v0");
        Map<String, String> actual = store.getAll(Arrays.asList("k0", "k1"));
        assert(actual.get("k1") == null);
        assert(actual.get("k0").equals("v0"));
    }

    @Test
    public void putAndGet() {
        store.put("k", "v");
        assert("v".equals(store.get("k")));
    }

    @Test
    public void doublePutAndGet() {
        String k = "k2";
        store.put(k, "v1");
        store.put(k, "v3");
        assert("v3".equals(store.get(k)));
    }
}
