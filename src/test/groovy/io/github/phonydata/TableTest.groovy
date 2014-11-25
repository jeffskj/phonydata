package io.github.phonydata;

import static org.junit.Assert.*

import org.junit.Test

class TableTest {

    @Test
    void canAddDataWithDefaultsAndColumnSensing() {
        Table t = new Table(name: 'my_table')
        
        t.addRow(foo: 'test', bar: 'foo')
        t.addRow(baz: false)
        t.addRow(yomama: 'sofat')
        
        t.defaults(bar: 'def-bar', blah:'zzz')
        
        def data = t.rows
        
        t.each { 
            println it
            assertEquals(5, it.data.keySet().size())
            assertTrue(it.columns.containsAll(['foo', 'bar', 'baz', 'yomama', 'blah']))
            assertEquals('zzz', it.blah)
        }
        
        assertEquals(3, data.size())
        
        assertEquals('test', data[0].foo)
        assertEquals('foo', data[0].bar)
        assertFalse(data[1].baz)
        assertNull(data[1].foo)
        assertNull(data[1].yomama)
    }
    
    @Test
    void canIncrementId() {
        Table t = new Table()
        t.id('id')
        t.addRow(name: 'joe')
        t.addRow(id: 3, name: 'jane')
        t.addRow(name: 'tim')
        
        def last = 0
        t.each {
            assertTrue(it.id > last)
            last = it.id 
        }
    }
    
    @Test
    void canReferenceOtherRows() {
        Table a = new Table(idColumn: 'id')
        def a7 = a.addRow(id: 7, name: 'abc')
        
        Table b = new Table(idColumn: 'id')
        b.addRow(a: a7, blah: 'abc')
        
        assertEquals(7, b.rows[0].a)
    }

    @Test(expected=IllegalStateException)
    void cantReferenceRowsWithNoId() {
        Table a = new Table()
        def a7 = a.addRow(id: 7, name: 'abc')
        
        Table b = new Table(idColumn: 'id')
        b.addRow(a: a7, blah: 'abc')
        
        b.rows[0].a
    }
    
    @Test
    void canUseClosureForValue() {
        Table t = new Table()
        t.defaults(now: { new Date() })
        t.addRow(x: 1)
        
        println t.rows
        assertNotNull(t.rows[0].now)  
    }
    
    @Test
    void canIterateRowsAndModify() {
        Table t = new Table()
        t.addRow(x:1)
        t.addRow(x:2)
        t.addRow(x:3)
        
        t.each { it.x = 7; it.y = 2 }
        
        t.each {
            println it
            assertEquals(7, it.x)
            assertEquals(2, it.y)
        }
    }
}
