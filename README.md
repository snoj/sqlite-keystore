sqlite-keystore
===============

A simple key store backed by SQLite


Usage
===============

##Start

```
var sqliteks = require('sqlite-keystore');
var ks = new sqliteks(':memory:');
```

##Insert
```
ks.insert(key, {date: something}, opts, callback(err, data))
```

##Update
```
ks.update(where, {date: somethingelse}, opts, callback(err, changedData))
```

##Remove
```
ks.remove(where)
```
##Select
```
ks.insert(key, {date: something}, opts, callback(err, data))
```


##'Where'
Where is based somewhat off MongoDB's way of querying.

###Simple =
```
sqliteks: {something: 'blue'}
sql expression: something = 'blue'
```

### (Greater|Less) Than (|Or Equal to)
```
sqliteks: {something: {$gt: 1}}
sql: something > 1

sqliteks: {something: {$ge: 1}}
sql: something >= 1

sqliteks: {something: {$lt: 1}}
sql: something < 1

sqliteks: {something: {$le: 1}}
sql: something <= 1
```

### Not equal
```
sqliteks: {something: {$ne: 1}}
sql: soemthing != 1
```