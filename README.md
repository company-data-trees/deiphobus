# deiphobus

[RingoJS](http://ringojs.org/) client for [Apache Cassandra](http://cassandra.apache.org/) inspired by the the [Helenus NodeJS](https://github.com/simplereach/helenus) and [Pelops JVM](https://github.com/s7/scale7-pelops) clients.

## Status

This client is still a work in progress, but already useful for many tasks.

The Selector object (ala Pelops) is relatively robust, and also provides a Map-Reduce/Hadoop split/iterate interface to efficiently traverse a column family (and/or keyspace). The Mutator class is currently limited to single key/CF updates. The connection pool is largely a placeholder, lacking connection pruning or server autodiscovery (the later is partially implemented). Also, the CQL interface is only schema-aware when used with a 1.0+ cluster.

## Examples

    var dei = require('deiphobus')

    var c = new dei.Connection({host: 'localhost'})
    c.use('foo')

    c.cql('select * from users')
    [{ key: 'alice', columns: {...}}, { key: 'zoe', columns: {...}}]

    var s = new dei.Selector(c)
    s.get('users', 'alice')
    [{ key: 'alice', columns: { password: '...', birth_year: 1987 }}]

    var m = new dei.Mutator(c);
    m.put('users', 'bob', { birth_year: 1992 })

    s.get('users', ['alice', 'bob'], {column: 'birth_year'})
    [{ key: 'alice', columns: { birth_year: 1987 }},
     { key: 'bob', columns: { birth_year: 1992 }}]


## Name

[Deiphobus](http://en.wikipedia.org/wiki/Deiphobus) was a brother of [Cassandra](http://en.wikipedia.org/wiki/Cassandra) and [Helenus](http://en.wikipedia.org/wiki/Helenus).
