#!/usr/bin/env ringo

var dei = require('.');

var c = new dei.Connection({});
c.use('foo');
var r = c.cql('select * from users');

require('ringo/shell').start();
