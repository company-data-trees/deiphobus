#!/usr/bin/env ringo

var dei = require('.');

var c = new dei.Connection({});
c.use('foo');
var r = c.cql('select * from users');
var s = new dei.Selector(c);
var m = new dei.Mutator(c);

var ss = s.splits('users');

require('ringo/shell').start();
