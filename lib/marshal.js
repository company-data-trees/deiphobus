
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
    return ByteBuffer.allocate(Math.ceil(bits/8));
}

function wrap(type, value, put) {
    var buffer = allocateBuffer(java.lang[type].SIZE);
    return buffer[put || ('put' + type)](value).rewind();
}

var UUID = require('./uuid');
var UUIDConverter = {
    from: UUID.fromByteBuffer,
    to: UUID.toByteBuffer
};

module.exports = {
    for: function(type) {
        return this[type] || this.BytesType;
    },
    
    AsciiType: {
        from: function(buffer) { return decodeString(buffer, ASCII); },
        to: function(string) { return encodeString(string, ASCII); }
    },

    UTF8Type: {
        from: function(buffer) { return decodeString(buffer, UTF8); },
        to: function(string) { return encodeString(string, UTF8); }
    },

    IntegerType: {
        from: function(buffer) { return buffer.getInt(); },
        to: function(number) { return wrap('Integer', number, 'putInt'); }
    },

    LongType: {
        from: function(buffer) { return buffer.getLong(); },
        to: function(number) { return wrap('Long', number); }
    },

    FloatType: {
        from: function(buffer) { return buffer.getFloat(); },
        to: function(number) { return wrap('Float', number); }
    },

    DoubleType: {
        from: function(buffer) { return buffer.getDouble(); },
        to: function(number) { return wrap('Double', number); }
    },

    BooleanType: {
        from: function(buffer) { return buffer.get() == 1; },
        to: function(boolean) { return wrap('Byte', boolean ? 1 : 0, 'put'); }
    },

    DateType: {
        from: function(buffer) { return new Date(buffer.getLong() / 1000); },
        to: function(date) {
            var time = date;
            if (date instanceof Date) time = date.getTime();
            return wrap('Long', time * 1000);
        }
    },

    BytesType: {
        from: function(buffer) { return new ByteArray(buffer.array()); },
        to: function(binary) { return ByteBuffer.wrap(binary.toByteArray()); }
    },

    CounterColumnType: {
        from: function(value) { return value; },
        to: function(value) { return value; }
    },

    LexicalUUIDType: UUIDConverter,
    TimeUUIDType: UUIDConverter,
    UUIDType: UUIDConverter
};
