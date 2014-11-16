package io.github.phonydata.reader;

import static org.junit.Assert.*

import org.junit.Test

class GroovyClosureDataSetReaderTest {

    @Test
    public void canReadFromClosure() {
        def clos = {
            people.id('id')
            people.defaults(city: 'Seattle')
            
            people(name: 'Joe Blow', age: 32)
            people(name: 'Jane Blow', age: 31)
            people(name: 'Tom Smith', age: 32)
        }
        
        def ds = new GroovyClosureDataSetReader(clos).read()
        assertEquals(1, ds.tables.size())
        
        ds.tables.people.each { println it }
        assertEquals(3, ds.tables.people.size())
        assertEquals(4, ds.tables.people.rows[0].columns.size())
    }

}
