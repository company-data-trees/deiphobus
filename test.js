#!/usr/bin/env ringo

var dei = require('.');

var c = new dei.Connection({});
c.use('foo');
var r = c.cql('select * from users');
var s = new dei.Selector(c);

// var cr = new dei.Connection({port: 9161});
// cr.use('Lists');

require('ringo/shell').start();
