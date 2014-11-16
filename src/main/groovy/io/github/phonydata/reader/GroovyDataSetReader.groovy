package io.github.phonydata.reader;

import io.github.phonydata.DataSet
import io.github.phonydata.Table



class GroovyDataSetReader implements DataSetReader {

    private InputStream input;

    GroovyDataSetReader(String text) {
        this(new ByteArrayInputStream(text.bytes))
    }
    
    GroovyDataSetReader(InputStream input) {
        this.input = input
    }
    
    DataSet read() {
        return new DataSet(tables: parse(input));
    }
        
    private Map<String, Table> parse(InputStream input) {
        GroovyShell shell = new GroovyShell()
        def script = shell.parse(input.newReader())
        
        def tables = [:]
        
        def getTable = { name ->            
            if (!tables[name]) {
                tables[name] = new Table(name: name)
            }
            return tables[name]
        }
        
        script.metaClass.methodMissing = { name, args ->
            if (args.size() > 1 || !(args[0] instanceof Map)) {
                throw new IllegalArgumentException('may only pass a single map to row method!')
            }
            return getTable(name).addRow(args[0])
        }
        
        script.metaClass.propertyMissing = { getTable(it) }
        
        script.run()
        
        return tables
    }
}