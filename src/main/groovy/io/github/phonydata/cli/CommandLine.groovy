package io.github.phonydata.cli

import io.github.phonydata.PhonyData
import io.github.phonydata.reader.GroovyDataSetReader

import java.sql.DriverManager

import javax.sql.DataSource

class CommandLine {
    public static void main(String[] args) {
        if (args.size() < 2) {
            println "Usage: phonydata <command>"
            return
        }
         
        def commandArgs = args[1..-1]
        if (args[0] == 'validate') {
            validate(args[1])
        } else if (args[0] == 'export') {
            def opts = commandArgs.collate(2).collectEntries { [it[0].replace('--', '').toLowerCase(), it[1]] }
            export(opts.url, opts.user, opts.pass, opts.driver, opts.tables)
        } else {
            println "Invalid command! Valid commands: validation, export"
        }
    }
    
    private static void export(String url, String user, String pass, String driverClass, String tables) {
        if (!url || !user || !pass || !driverClass || !tables) {
            println "url, user, pass, driverClass, and tables are required to export data!"
            println "Usage: phonydata export --url <jdbc url> --user <username> --pass <password> --driver <driver class name> --tables <comma-separated table names>"
            return
        }
        
        Class.forName(driverClass, true, ClassLoader.systemClassLoader)
        def ds = [getConnection: { DriverManager.getConnection(url, user, pass) }] as DataSource
        
        def out = new ByteArrayOutputStream()
        PhonyData.from(ds, tables.split(',') as List).into(out)
        println out
    }
    
    private static void validate(String filename) {
        def ds = new GroovyDataSetReader(new File(filename).text).read()
        
        println "Dataset Summary:"
        println '-' * 40
        println "${ds.tables.size()} tables"
        println ''
        
        ds.tables.each { n, table -> 
            println "Table: $n"
            println "Columns: ${table.columns.join(',')}"
            println "Num Rows: ${table.size()}"
            println ''
        }
        
    }
}
