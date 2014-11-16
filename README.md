PhonyData
=========

[![Build Status](https://travis-ci.org/jeffskj/phonydata.svg?branch=master)](https://travis-ci.org/jeffskj/phonydata)

PhonyData is a library to import and export test datasets for things like unit testing databases or local dev data. 

Features
--------

  * Clear, consise groovy based DSL to define datasets
  * Can both import and export data
  * Automatically escapes sql keywords
  * Automatically topologically sorts tables with foreign keys
  * Auto column sensing
  * Specify default column values
  * ID auto generation
  * Row references
  * Define dataset inline in groovy tests 

DSL Overview
------------
  * call an undefined method or reference a property to specify a table
  * Groovy map syntax to pass column values `my_table(id:1, column1: 'test')`
  * Specify an ID column to enable row references or auto id incrementing `my_table.id('id')`
  * Specify defaults column values `my_table.defaults(some_column:'default value', column1: 'default')`

DSL Example
-----------

    // specify id columns
    address.id('id') 
    people.id('id')
    
    // specify columns that will be the same for all rows
    people.defaults(birthday: new Date())
    
    people(name: 'Jane Doe')
    
    100.times { n ->
        def person = people(name: 'joe blow' + n) // returns reference to row which will be de-referenced by id
        
        address(street: "$n main st".toString(), city: 'seattle', state: 'wa', 
                person: person /* holds actual value of person.id */)
                
        address(street: "$n oak st".toString(), city: 'seattle', state: 'wa', person: person)    
    }
    
Usage Example
-------------
    
    DataSource datasource = ... // get reference to JDBC DataSource
    def dataset = getClass().classLoader.getResourceAsStream('/path/to/dataset') // may also be a String
    PhonyData.readInto(dataset, datasource) // reads data from groovy dataset and writes to datasource
    
    // reads the data from specified tables and writes a groovy dataset to output stream
    PhonyData.readFrom(datasource, ['table1','table2'], new File('/some/file').newOutputStream())

   
Inline Groovy Example
---------------------

     @Before
     public void setup() {     
        PhonyData.readInto(dataSource) {
            people(name: 'Jane Doe')
            address(street: '123 main st')
        }
    }

Get It!
-------

Add this dependency to your project:

    <dependency>
       <groupId>io.github.phonydata</groupId>
       <artifactId>phonydata</artifactId>
       <version>0.1</version>
    </dependency>
