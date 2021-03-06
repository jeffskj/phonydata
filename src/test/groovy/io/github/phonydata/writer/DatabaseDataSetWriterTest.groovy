package io.github.phonydata.writer;

import static org.junit.Assert.*
import io.github.phonydata.reader.GroovyDataSetReader

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
        def ds = new GroovyDataSetReader("keys(id:1, key: 'unlocksaysme')").read()
        def writer = new DatabaseDataSetWriter(createDataSource(), false)
        writer.sql.executeUpdate("create table keys(id int primary key, \"KEY\" varchar(255))")
        writer.write(ds)
        assertEquals(1, writer.sql.rows("select * from keys").size())
    }
    
    @Test
    void canInsertMoreComplexDataSet() {
        def ds = new GroovyDataSetReader(complexDataSet).read()
        def writer = new DatabaseDataSetWriter(createDataSource(), true)
        writer.sql.executeUpdate("create table unused(id int primary key, person int)")
        
        writer.sql.executeUpdate("create table people(id int primary key, name varchar(255))")
        writer.sql.executeUpdate("create table address(id int primary key, street varchar(255), city varchar(255), state varchar(2), person int)")
        
        writer.sql.executeUpdate("alter table address add foreign key (person) references people(id)")
        writer.sql.executeUpdate("alter table unused add foreign key (person) references people(id)")
        writer.write(ds)
        
        assertEquals(100, writer.sql.rows("select * from people").size())
        assertEquals(200, writer.sql.rows("select * from address").size())
    }
    
    private DataSource createDataSource() {
        return JdbcConnectionPool.create("jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1", "sa", "sa").dataSource
    }
    
    private def complexDataSet = '''
address.id('id')
People.id('id')

100.times { n ->
    def person = People(name: 'joe blow' + n)
    address(street: "$n main st".toString(), city: 'seattle', state: 'wa', person: person)
    address(street: "$n oak st".toString(), city: 'seattle', state: 'wa', person: person)    
}
'''
}
