package lyd.ai.native4j.jdbc.data.type;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.TimeUnit;

import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;

public class DataTypeDate implements IDataType {

    private static final Date DEFAULT_VALUE = new Date(0);
    private static final long MILLIS_DIFF = TimeUnit.DAYS.toMillis(1);

    @Override
    public String name() {
        return "Date";
    }

    @Override
    public int sqlTypeId() {
        return Types.DATE;
    }

    @Override
    public Object defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof Date, "Expected Date Parameter, but was " + data.getClass().getSimpleName());

        serializer.writeShort((short) (((Date) data).getTime() / MILLIS_DIFF));
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return new Date(deserializer.readShort() * MILLIS_DIFF);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException {
        Date[] data = new Date[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = new Date(deserializer.readShort() * MILLIS_DIFF);
        }
        return data;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.StringLiteral, "Expected String Literal.");
        String dateString = token.data();

        String[] yearMonthDay = dateString.split("-", 3);

        return new Date(
            Integer.valueOf(yearMonthDay[0]) - 1900,
            Integer.valueOf(yearMonthDay[1]) - 1,
            Integer.valueOf(yearMonthDay[2]));
    }
}
