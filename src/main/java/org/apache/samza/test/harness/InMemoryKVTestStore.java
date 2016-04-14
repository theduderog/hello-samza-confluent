package org.apache.samza.test.harness;

import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.lang.NotImplementedException;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

//mostly copied from org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore

public class InMemoryKVTestStore<K, V> implements KeyValueStore<K, V> {
    TreeMap<K,V> underlying = new TreeMap<K, V> ();//TODO: change this

    public void flush(){
        //do nothing
    }

    public void close(){
        //do nothing
    }

    private class InMemoryIterator implements KeyValueIterator<K, V> {

        Iterator<Map.Entry<K, V>> iter;

        public InMemoryIterator(Iterator<Map.Entry<K, V>> iter){
            this.iter = iter;
        }

        public void close(){
            //do nothing
        }

        public void remove(){
            iter.remove();
        }

        public Entry<K, V> next(){
            Map.Entry<K, V> n = iter.next();
            return new Entry(n.getKey(), n.getValue());
        }

        public boolean hasNext() {
            return iter.hasNext();
        }

    /*
    * This method is supposed to be called only after an iterator is created first
    * using the store's newIterator position. For some stores, the creation of the
    * */
        public void seekToFirst() {
            throw new UnsupportedOperationException();
        }

        public void seek(K key){
            throw new UnsupportedOperationException();
        }
    }

    public KeyValueIterator<K, V> all(){
        return new InMemoryIterator(underlying.entrySet().iterator());
    }

    //Note that in our use case we don't need to range function anyway, that is why we decide not to implement it
    public KeyValueIterator<K, V> range(K from, K to) {
        throw new NotImplementedException();
        /*
        assert(from != null && to != null);

        return new InMemoryIterator(underlying.subMap(from, to).entrySet().iterator());
        */
    }

    public void delete(K key){
        put(key, null);
    }

    public void deleteAll(List<K> keys){
        KeyValueStore.Extension.deleteAll(this, keys);
    }

    public void putAll(List<Entry<K, V>> entries){
        Iterator<Entry<K, V>> iter = entries.iterator();

        while(iter.hasNext()) {
            Entry<K, V> next = iter.next();
            put(next.getKey(), next.getValue());
        }
    }

    public void put(K key, V value){

        assert(key != null);
        if (value == null) {
            underlying.remove(key);
        } else {
            underlying.put(key, value);
        }
    }

    public V get(K key){
        assert(key != null);
        V found = underlying.get(key);
        return found;
    }

    public Map<K, V> getAll(List<K> keys){
        return KeyValueStore.Extension.getAll(this, keys);
    }
}
