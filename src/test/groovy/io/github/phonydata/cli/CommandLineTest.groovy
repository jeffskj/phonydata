package io.github.phonydata.cli;

import static org.junit.Assert.*
import groovy.sql.Sql

import javax.sql.DataSource

import org.h2.Driver
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class CommandLineTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder()
    
    @Test
    public void testExport() {
        def url = "jdbc:h2:mem:test${UUID.randomUUID()};DB_CLOSE_DELAY=-1"
        def ds = createDataSource(url)
        def sql = new Sql(ds)
        sql.executeUpdate("create table testing(col varchar(255))")
        sql.executeInsert("insert into testing (col) values ('value')")
        
        def out = ''
        CommandLine.metaClass.'static'.println = { out = it.toString() }
        CommandLine.main("export --url ${url} --user sa --pass sa --driver ${Driver.class.name} --tables testing".split(' ') as String[])
        println out
        assertTrue(out.contains('testing('))
    }
    
    @Test
    public void canValidate() {
        def out = ''
        CommandLine.metaClass.'static'.println = { out += "${it.toString()}\n" }
        
        def f = tmp.newFile()
        f.text = 'testing(COL1:"value")\ntesting(COL2:"value")'
        
        CommandLine.main("validate ${f.absolutePath}".split(' ') as String[])
        
        println out
        assertTrue(out.contains("testing"))
        assertTrue(out.contains("COL1,COL2"))
    }

    private DataSource createDataSource(url) {
        return JdbcConnectionPool.create(url, "sa", "sa").dataSource
    }
}
