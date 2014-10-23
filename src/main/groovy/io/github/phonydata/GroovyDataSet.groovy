package io.github.phonydata;



class GroovyDataSet implements DataSet {

    private Map<String, Table> tables;

    GroovyDataSet(String text) {
        this(new ByteArrayInputStream(text.bytes))
    }
    
    GroovyDataSet(InputStream input) {
        this.tables = parse(input);
    }
    
    Map<String, Table> getTables() {
        return tables;
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
    
    static abstract class DataSetBuilder extends Script {
        def methodMissing(name, args) {
            println args
            return getTable(name).addRow(args)
        }
        
        def propertyMissing(name) { getTable(name) }
        
    }
}
