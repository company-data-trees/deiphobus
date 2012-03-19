var ByteBuffer = java.nio.ByteBuffer;
var Charset = java.nio.charset.Charset;

var ASCII = Charset.forName('US-ASCII');
var UTF8 = Charset.forName('UTF-8');

function decodeString(buffer, charset) {
    return charset.newDecoder().decode(buffer.duplicate()).toString();
}

function encodeString(string, charset) {
    return ByteBuffer.wrap(string.toBytes(charset));
}

function allocateBuffer(bits) {
    return java.nio.ByteBuffer.allocate(Math.ceil(bits/8));
}

function wrap(type, value) {
    var buffer = allocateBuffer(java.lang[type].SIZE);
    buffer['put' + type](value);
    buffer.rewind();
    return buffer;
}

module.exports = {
    AsciiType: {
        from: function(buffer) { return decodeString(buffer, ASCII); },
        to: function(string) { return encodeString(string, ASCII); }
    },

    UTF8Type: {
        from: function(buffer) { return decodeString(buffer, UTF8); },
        to: function(string) { return encodeString(string, UTF8); }
    },

    LongType: {
        from: function(buffer) { return buffer.getLong(); },
        to: function(long) { return wrap('Long', long); }
    }


};
