package io.github.phonydata;

import io.github.phonydata.reader.DataSetReader
import io.github.phonydata.reader.DatabaseDataSetReader
import io.github.phonydata.reader.FlatXmlDataSetReader
import io.github.phonydata.reader.GroovyClosureDataSetReader
import io.github.phonydata.reader.GroovyDataSetReader
import io.github.phonydata.writer.DatabaseDataSetWriter
import io.github.phonydata.writer.GroovyDataSetWriter

import javax.sql.DataSource

public class PhonyData {

    private List<DataSetReader> readers
    
    public static PhonyData from(Closure clos) {
        return new PhonyData(readers: clos ? [new GroovyClosureDataSetReader(clos)] : []) 
    }
    
    public static PhonyData from(String groovyDataset) {
        return new PhonyData(readers: groovyDataset ? [new GroovyDataSetReader(groovyDataset)] : [])
    }
    
    public static PhonyData from(InputStream groovyDataset) {
        return new PhonyData(readers: groovyDataset ? [new GroovyDataSetReader(groovyDataset)] : [])
    }
    
    public static PhonyData fromXml(String flatXml) {
        return new PhonyData(readers: flatXml ? [new FlatXmlDataSetReader(flatXml)] : [])
    }
    
    public static PhonyData fromXml(InputStream flatXml) {
        return new PhonyData(readers: flatXml ? [new FlatXmlDataSetReader(flatXml)] : [])
    }
    
    public static PhonyData from(DataSource ds, Collection<String> tables) {
        return new PhonyData(readers: [new DatabaseDataSetReader(ds, tables)])
    }
    
    public PhonyData andFrom(Closure clos) {
        if (clos) {
            readers << new GroovyClosureDataSetReader(clos)
        }
        return this
    }
    
    public PhonyData andFrom(String groovyDataset) {
        if (groovyDataset) {
            readers << new GroovyDataSetReader(groovyDataset)
        }
        return this
    }
    
    public PhonyData andFrom(InputStream groovyDataset) {
        if (groovyDataset) {
            readers << new GroovyDataSetReader(groovyDataset)
        }
        return this
    }
    
    public PhonyData andFromXml(String flatXml) {
        if (flatXml) {
            readers << new FlatXmlDataSetReader(flatXml)
        }
        return this
    }
    
    public PhonyData andFromXml(InputStream flatXml) {
        if (flatXml) {
            readers << new FlatXmlDataSetReader(flatXml)
        }
        return this
    }
    
    public PhonyData andFrom(DataSource ds, Collection<String> tables) {
        readers << new DatabaseDataSetReader(ds, tables)
        return this
    }
    
    public void into(DataSource dest) {
        into(dest, false)
    }

    public void into(DataSource dest, boolean clean) {
        readers.each { reader ->
            def writer = new DatabaseDataSetWriter(dest, clean)
            writer.write(reader.read())
        }
    }

    public void into(OutputStream dest) {
        readers.each { reader ->
            def writer = new GroovyDataSetWriter(dest)
            writer.write(reader.read())
        }
    }
}