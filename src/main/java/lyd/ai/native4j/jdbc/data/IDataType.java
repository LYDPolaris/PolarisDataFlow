package lyd.ai.native4j.jdbc.data;

import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public interface IDataType {
    String name();

    int sqlTypeId();

    Object defaultValue();

    Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException;

    void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException;

    Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException;

    void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException;

    Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException;
}
