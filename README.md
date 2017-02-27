# MiniDatabase
The Mini database could be used to make SQL statement come true without additional library, including inserting, deleting, searching, aggregation functions and so on. With the help of data storing, multi-thread controlling, we can have a good way to manage resources on the operating system. In order to recover from the disaster, we need to use a better strategy to manage the buffer pool including FIFO, LRU and so on.
It focused on basics such as the relational algebra and data model, schema normalization, query optimization, and transactions. ACID(Atomicity, Consistency, Isolation, Durability) are implemented in java except "Consistency".

## Technology
Java, Multi-thread Programming, SQL, Database System Implementation, Object-Oriented, JUnit Test, Ant

## Modules
SimpleDB consists of:
+ Classes that represent fields, tuples, and tuple schemas;
+ Classes that apply predicates and conditions to tuples;
+ One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations;
+ A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples;
+ A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions (neither of which you need to worry about for this lab); and,
+ A catalog that stores information about available tables and their schemas.

## How to use it
ant         Build the default target (for simpledb, this is dist).
ant -projecthelp      List all the targets in build.xml with descriptions.
ant dist    Compile the code in src and package it in dist/simpledb.jar.
ant test    Compile and run all the unit tests.
ant runtest -Dtest=testname Run the unit test named testname.
ant systemtest  Compile and run all the system tests.
ant runsystest -Dtest=testname Compile and run the system test named testname.

## SQL
+ SELECT g.title
+ FROM grants gWHERE g.title 
+ LIKE 'Monkey'
+ 

+ SELECT g.title
+ FROM grants g, researchers r, grant_researchers gr
+ WHERE r.name = 'Samuel Madden' AND gr.researcherid = r.id AND gr.grantid = g.id;
+ 

+ SELECT r2.name, count(g.id)
+ FROM grants g, researchers r, researchers r2, grant_researchers gr, grant_researchers gr2
+ WHERE r.name = 'Samuel Madden'AND gr.researcherid = r.idAND gr.grantid = g.idAND gr2.researcherid = r2.idAND gr.grantid = gr2.grantid 
+ GROUP BY r2.name 
+ ORDER BY r2.name;
+ 

## Transactions, Locking, and Concurrency Control
Modifications from a transaction are written to disk only after it commits. This means we can abort a transaction by discarding the dirty pages and rereading them from disk. Thus, we must not evict dirty pages. 
This policy is called NO STEAL.
It is possible for transactions in SimpleDB to deadlock. Time counting or dependency graph can be used to deal with deadlock problems.

## References
[MIT 6.830](https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010/index.htm)
