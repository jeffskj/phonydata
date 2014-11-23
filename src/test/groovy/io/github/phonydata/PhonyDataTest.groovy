package io.github.phonydata;

import static org.junit.Assert.*
import groovy.sql.Sql

import javax.sql.DataSource

import org.h2.jdbcx.JdbcConnectionPool
import org.junit.Test

class PhonyDataTest {

    @Test
    public void testFromClosure() {
        assertDataWritten(PhonyData.from { table1(col1:'blah') })
    }

    @Test
    public void testFromString() {
        assertDataWritten(PhonyData.from("table1(col1:'blah')"))
    }

    @Test
    public void testFromInputStream() {
        assertDataWritten(PhonyData.from(new ByteArrayInputStream("table1(col1:'blah')".bytes)))
    }

    @Test
    public void testFromXmlString() {
        assertDataWritten(PhonyData.fromXml('<dataset><table1 col="val"/></dataset>'))
    }

    @Test
    public void testFromXmlInputStream() {
        assertDataWritten(PhonyData.fromXml(new ByteArrayInputStream('<dataset><table1 col="val"/></dataset>'.bytes)))
    }

    @Test
    public void canReadFromDb() {
        def ds = createDataSource()
        def sql = new Sql(ds)
        sql.executeUpdate("create table testing(col varchar(255))")
        sql.executeInsert("insert into testing (col) values ('value')")
        assertDataWritten(PhonyData.from(ds, ['testing']))
    }
    
    @Test
    public void canWriteToDb() {
        def ds = createDataSource()
        def sql = new Sql(ds)
        sql.executeUpdate("create table testing(col varchar(255))")
        PhonyData.from { testing(col: 'val') }.into(ds)
        assertEquals(1, sql.rows('select * from testing').size())
    }

    private void assertDataWritten(PhonyData pd) {
        def out = new ByteArrayOutputStream()
        pd.into(out)
        println out
        assertTrue(out.size() > 0)
    }
    
    private DataSource createDataSource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1", "sa", "sa").dataSource
    }
}
