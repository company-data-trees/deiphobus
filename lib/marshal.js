
var ByteArray = require('binary').ByteArray;

var ByteBuffer = java.nio.ByteBuffer;
var Charset = java.nio.charset.Charset;

var ASCII = Charset.forName('US-ASCII');
var UTF8 = Charset.forName('UTF-8');

function decodeString(buffer, charset) {
    return charset.newDecoder().decode(buffer).toString();
}

function encodeString(string, charset) {
    var javaString = new java.lang.String(string);
    return ByteBuffer.wrap(javaString.getBytes(charset));
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

var Marshal = {
    for: function(type) {
        if (typeof type == 'object') {
            return new this.CompositeType(type);
        }
        return this[type] || this.BytesType;
    },
    
    CompositeType: function(fields) {
        this.fields = fields.map(function(field) {
            return Marshal.for(field);
        });

        this.from = function(buffer) {
            var parts = [];
            var slice = buffer.slice();
            for (var i = 0; i < this.fields.length; i++) {
                if (slice.position() == slice.capacity()) break;
                slice.limit(slice.position() + 2);
                var length = slice.getShort();
                slice.limit(slice.position() + length);
                parts.push(this.fields[i].from(slice));
                slice.limit(slice.position() + 1);
                var endByte = slice.get();
            }
            return parts;
        }

        this.to = function(parts) {
            var size = 0;
            var partBuffers = [];
            for (var i = 0; i < this.fields.length; i++) {
                var partBuffer = this.fields[i].to(parts[i]);
                partBuffers.push(partBuffer);
                size += (partBuffer.limit() - partBuffer.position()) + 3;
            }

            var buffer = ByteBuffer.allocate(size);
            partBuffers.forEach(function(partBuffer) {
                var partSize = partBuffer.limit() - partBuffer.position();
                buffer.putShort(partSize);
                buffer.put(partBuffer);
                buffer.put(0);
            });

            return buffer.rewind();
        }
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
        // encoded as a variable length byte array
        from: function(buffer) {
            var value = 0;
            while (buffer.remaining() > 0) {
                var byte = buffer.get();
                value = (value << 8) | byte;
            }
            return value;
        },
        to: function(number) {
            var buffer = wrap('Long', number);
            // skip leading 0 bytes
            var byte = 0;
            while (buffer.remaining() > 1 && byte == 0)
                 byte = buffer.get();
            if (byte != 0) buffer.position(buffer.position() - 1);
            return buffer;
        }
    },

    Int32Type: {
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
        from: function(buffer) {
            var length = buffer.limit() - buffer.position();
            var bytes = java.lang.reflect.Array.newInstance(java.lang.Byte.TYPE, length);
            buffer.get(bytes);
            return new ByteArray(bytes);
        },
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

module.exports = Marshal;

ByteArray.prototype.asByteBuffer = function() {
    return Marshal.BytesType.to(this);
}

ByteArray.prototype.asString = function() {
    return Marshal.UTF8Type.from(this.asByteBuffer());
}

ByteArray.prototype.asLong = function() {
    return Marshal.LongType.from(this.asByteBuffer());
}

ByteArray.prototype.asDouble = function() {
    return Marshal.DoubleType.from(this.asByteBuffer());
}
