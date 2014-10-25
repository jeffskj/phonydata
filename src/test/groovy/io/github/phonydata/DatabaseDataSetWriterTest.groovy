package io.github.phonydata;

import static org.junit.Assert.*
import groovy.sql.Sql

import javax.sql.DataSource

import org.h2.jdbcx.JdbcConnectionPool
import org.junit.Test

class DatabaseDataSetWriterTest {

    @Test
    void canTopologicalSort() {
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
    
    @Test
    void canInsertSimpleDataSet() {
        def ds = new GroovyDataSet("test(id:1, name: 'joe blow')")
        def writer = new DatabaseDataSetWriter(createConnection())
        writer.write(ds)
        assertEquals(1, writer.sql.rows("select * from test").size())
    }
    
    private DataSource createConnection() {
        Class.forName("org.h2.Driver");
        JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "sa");
        def sql = new Sql(cp.dataSource)
        sql.cacheConnection = true
        sql.executeUpdate("create table test (id int primary key, name varchar(255))")
        return cp.dataSource
    }
}
