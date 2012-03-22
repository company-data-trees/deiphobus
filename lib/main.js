var fs = require('fs');

var jarPath = fs.resolve(module.path, '../jars');
fs.listTree(jarPath).forEach(function(path) {
    path = jarPath + '/' + path;
    if (fs.isFile(path)) addToClasspath(path);
});

module.exports = {
    version: '0.0.1',
    Connection: require('./connection.js'),
    Pool: require('./pool.js'),
    Marshal: require('./marshal.js'),
    UUID: require('./uuid.js'),
    Selector: require('./selector.js')
};
