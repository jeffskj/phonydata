package io.github.phonydata;

import static org.junit.Assert.*

import org.junit.Test

class GroovyDataSetWriterTest {

    @Test
    public void canWriteReadableGroovyDataSet() {
        def ds = new GroovyDataSet(dataSet)
        def out = new StringWriter()
        def writer = new GroovyDataSetWriter(out)
        writer.write(ds)
        
        println out.toString()
        
        def read = new GroovyDataSet(out.toString())
        assertEquals(ds.tables.size(), read.tables.size())
        assertEquals(ds.tables['address'].rows.size(), read.tables['address'].rows.size())
        assertEquals(ds.tables['people'].rows.size(), read.tables['people'].rows.size())
    }
    
    private def dataSet = '''
address.id('id')
people.id('id')

100.times { n ->
    def person = people(name: 'joe blow' + n)
    address(street: "$n main st".toString(), city: 'seattle', state: 'wa', person: person)
    address(street: "$n oak st".toString(), city: 'seattle', state: 'wa', person: person)    
}
'''
}
