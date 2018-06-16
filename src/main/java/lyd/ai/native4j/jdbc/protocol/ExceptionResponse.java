package lyd.ai.native4j.jdbc.protocol;

import lyd.ai.native4j.jdbc.ClickHouseSQLException;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class ExceptionResponse extends RequestOrResponse {

    ExceptionResponse() {
        super(ProtocolType.RESPONSE_Exception);
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        throw new UnsupportedOperationException("ExceptionResponse Cannot write to Server.");
    }

    public static SQLException readExceptionFrom(BinaryDeserializer deserializer) throws IOException {
        int code = deserializer.readInt();
        String name = deserializer.readStringBinary();
        String message = deserializer.readStringBinary();
        String stackTrace = deserializer.readStringBinary();

        if (deserializer.readBoolean()) {
            return new ClickHouseSQLException(
                code, name + message + ". Stack trace:\n\n" + stackTrace, readExceptionFrom(deserializer));
        }

        return new ClickHouseSQLException(code, name + message + ". Stack trace:\n\n" + stackTrace);
    }
}
