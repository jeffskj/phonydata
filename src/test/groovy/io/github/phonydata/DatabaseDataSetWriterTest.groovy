package io.github.phonydata;

import static org.junit.Assert.*

import org.junit.Test

class DatabaseDataSetWriterTest {

    @Test
    public void canTopologicalSort() {
        def sorted = DatabaseDataSetWriter.topologicalSort(a: [], b: [], c:[])
        assertEquals(['a', 'b', 'c'], sorted)
        
        sorted = DatabaseDataSetWriter.topologicalSort(a: ['b'], b: [], c:[])
        assertEquals('b', sorted.first())
        
        sorted = DatabaseDataSetWriter.topologicalSort(a: ['b'], b: ['c'], c:[])
        assertEquals(['c', 'b', 'a'], sorted)
        
        sorted = DatabaseDataSetWriter.topologicalSort(a: [], b: ['c', 'a'], c:['a'])
        assertEquals(['a', 'c', 'b'], sorted)
    }

    @Test(expected=RuntimeException)
    void disallowsSortingCyclicalGraph() {
        DatabaseDataSetWriter.topologicalSort(a: ['b'], b: ['c'], c:['a'])
    }
}
