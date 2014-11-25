package io.github.phonydata.reader;

import io.github.phonydata.DataSet
import io.github.phonydata.Table

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class GroovyClosureDataSetReader implements DataSetReader {
    private Logger logger = LoggerFactory.getLogger(getClass())

    private Closure closure;

    GroovyClosureDataSetReader(Closure clos) {
        this.closure = clos
    }
    
    DataSet read() {
        return new DataSet(tables: parse(closure));
    }
        
    private Map<String, Table> parse(Closure clos) {
        def delegate = new Delegate()
        closure.delegate = delegate
        closure()
        
        logger.info ("read groovy closure dataset with {} tables and {} total rows", 
            delegate.tables.size(), delegate.tables.values().sum { it.rows.size() })
        
        return delegate.tables
    }
    
}

class Delegate {
    def tables = [:]
            
    def getTable(name) {
        if (!tables[name]) {
            tables[name] = new Table(name: name)
        }
        return tables[name]
    }
    
    def methodMissing(String name, args) {
        if (args.size() > 1 || !(args[0] instanceof Map)) {
            throw new IllegalArgumentException('may only pass a single map to row method!')
        }
        return getTable(name).addRow(args[0])
    }
    
    def propertyMissing(String name) { getTable(name) }
}