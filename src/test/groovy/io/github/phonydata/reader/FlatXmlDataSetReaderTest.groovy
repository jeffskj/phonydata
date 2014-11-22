package io.github.phonydata.reader;

import static org.junit.Assert.*

import org.junit.Test

class FlatXmlDataSetReaderTest {

    @Test
    public void canReadToDataSet() {
        def data = new FlatXmlDataSetReader(XML).read()
        
        assertEquals(2, data.tables.size())
        assertEquals(1, data.tables.people.size())
        
        assertTrue(data.tables.people.rows[0].born instanceof java.sql.Date)
        assertTrue(data.tables.people.rows[0].time instanceof java.sql.Time)
        assertTrue(data.tables.people.rows[0].ts instanceof java.sql.Timestamp)
        
        assertEquals(1, data.tables.address.size())
    }

    static def XML = """
<dataset>
    <people id="1" name="Joe Blow" born="1985-12-01" time="11:20:45" ts="1985-12-01 11:20:45.1234" />
    <address id="1" person="1" address="123 main st" city="seattle" />
</dataset>"""
}
