---
layout: global
title: SHOW DATABASES
displayTitle: SHOW DATABASES
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Description
Lists the databases that match an optionally supplied string pattern. If no
pattern is supplied then the command lists all the databases in the system.
Please note that the usage of `SCHEMAS` and `DATABASES` are interchangable
and mean the same thing.

### Syntax
{% highlight sql %}
SHOW {DATABASES|SCHEMAS} [LIKE 'pattern']
{% endhighlight %}

### Parameters
##### ***LIKE 'pattern'***:
A string pattern that is used to match the databases. In pattern, `*` matches any number of characters.

### Examples
{% highlight sql %}
-- Lists all the databases.
SHOW DATABASES;
-- Lists databases with name starting with string pattern `test`
SHOW DATABSES LIKE 'test*';
-- Lists all databases. Keywords SCHEMAS and DATABASES are interchangeable. 
SHOW SCHEMAS;
{% endhighlight %}
### Related Statements
- [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-databases.html)
- [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
- [ALTER DATABASE](sql-ref-syntax-ddl-alter-database.html)
