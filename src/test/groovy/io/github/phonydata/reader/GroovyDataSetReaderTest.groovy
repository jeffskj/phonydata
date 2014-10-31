package io.github.phonydata.reader;

import static org.junit.Assert.*

import org.junit.Test

class GroovyDataSetReaderTest {

    @Test
    public void canParseSimpleDataSet() {
        def text = '''
people.defaults(age: 30)

people(id: 1, name: 'Joe Blow')
people(id: 2, name: 'Jane Blow')
people.each { it.state = 'WA' }

'''
        def tables = new GroovyDataSetReader(text).read().tables
        
        tables.people.rows.each { println it }
        
        assertEquals(1, tables.size())
        assertEquals(2, tables.people.rows.size())
        assertEquals(30, tables.people.rows[0].age)
    }
    
    @Test
    public void canParseComplicatedDataSet() {
        def text = '''
people.id('id')
people.defaults(age: 30)

def joe = people(name: 'Joe Blow')
def jane = people(name: 'Jane Blow')

address(city:'seattle', person:joe)
address(city:'new york', person:jane)
'''
        def tables = new GroovyDataSetReader(text).read().tables
        
        tables.people.rows.each { println it }
        tables.address.rows.each { println it }
        
        assertEquals(2, tables.size())
        assertEquals(2, tables.people.rows.size())
        assertEquals(2, tables.address.rows.size())
    }
}
