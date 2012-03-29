#!/usr/bin/env ringo

var dei = require('.');

var c = new dei.Connection({});
c.use('foo');
var r = c.cql('select * from users');
var s = new dei.Selector(c);
var m = new dei.Mutator(c);

var ss = s.splits('users');

var cr = new dei.Connection({port: 9161});
cr.use('Import');
var sr = new dei.Selector(cr);
var xr = sr.get('UnifiedCompanyData', {start: '0', end: '0'})

require('ringo/shell').start();
