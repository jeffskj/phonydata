package io.github.phonydata.writer;

import static org.junit.Assert.*
import io.github.phonydata.reader.GroovyClosureDataSetReader
import io.github.phonydata.reader.GroovyDataSetReader

import org.junit.Test

class GroovyDataSetWriterTest {

    @Test
    public void canWriteReadableGroovyDataSet() {
        def ds = new GroovyClosureDataSetReader({
            address.id('id')
            people.id('id')
            
            someTable(pattern: /foo\d+/)
            
            100.times { n ->
                def person = people(name: 'joe blow' + n, active: n % 2 == 0, born: new Date())
                address(street: "$n main st".toString(), city: 'seattle', state: 'wa', person: person)
                address(street: "$n oak st".toString(), city: 'seattle', state: 'wa', person: person)    
            }
        }).read()
        
        def out = new StringWriter()
        def writer = new GroovyDataSetWriter(out)
        writer.write(ds)
        
        println out.toString()
        
        def read = new GroovyDataSetReader(out.toString()).read()
        assertEquals(ds.tables.size(), read.tables.size())
        assertEquals(ds.tables['address'].rows.size(), read.tables['address'].rows.size())
        assertEquals(ds.tables['people'].rows.size(), read.tables['people'].rows.size())
        assertEquals(50, ds.tables['people'].rows.findAll { it.data.active == true }.size())
        assertEquals(50, ds.tables['people'].rows.findAll { it.data.active == false }.size())
        
    }
}
