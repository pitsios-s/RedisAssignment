# RedisAssignment
The first assignment of the graduate course "Big Data Systems"

### Description
This implementation consists of 2 parts. 
* Part 1 :  Given a input file that describes an SQL *INSERT* statement in a fixed format, create a program that reads this file and translates this SQL-like syntax, into Redis commands.

* Part 2: Given a input file that describes an SQL *SELECT* statement in a fixed format, create a program that reads this file, gets the neccessary data from Redis and filters the results based on the SQL *WHERE* clause.

The implementation is in python and the scripts can be found under __src__ directory.

### Input files format.

The file that contains the insert statements, should be in the following format:

1. First line contains the name of the relation.
2. Second line contains the primary key of the relation.
3. All the rest lines contain the name of the different attributes, one per line.
4. A semicolon indicating the end of the relation's description.
5. Lines containing values for the columns, separated by semicolon.

An example is given below:
```
employee
ssn
firstName
lastName
;
123;Stamatis;Pitsios
456;Foo1;Foo2
789;Foo3;Foo4
```

The format of the query file, is the following:

1. First line contains a comma-separated list of the names of the attributes that we want to project.
2. The second line contains a comma-separated list with the names of the relations.
3. Third line contains an SQL-like condition.

For example:
```
firstName, lastName
employee
employee.ssn = 123
```

Some sample input files, are given under folder __res__
