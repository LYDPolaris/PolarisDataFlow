package lyd.ai.native4j.jdbc.data.type.complex;

import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;

public class DataTypeDateTime implements IDataType {

    private static final Timestamp DEFAULT_VALUE = new Timestamp(0);

    private final int offset;
    private final String name;

    public DataTypeDateTime(String name, TimeZone timeZone) {
        this.name = name;
        long now = System.currentTimeMillis();
        this.offset = TimeZone.getDefault().getOffset(now) - timeZone.getOffset(now);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.TIMESTAMP;
    }

    @Override
    public Object defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.StringLiteral, "Expected String Literal.");
        String timestampString = token.data();

        String[] dateAndTime = timestampString.split(" ", 2);
        String[] yearMonthDay = dateAndTime[0].split("-", 3);
        String[] hourMinuteSecond = dateAndTime[1].split(":", 3);

        return new Timestamp(
            Integer.valueOf(yearMonthDay[0]) - 1900,
            Integer.valueOf(yearMonthDay[1]) - 1,
            Integer.valueOf(yearMonthDay[2]),
            Integer.valueOf(hourMinuteSecond[0]),
            Integer.valueOf(hourMinuteSecond[1]),
            Integer.valueOf(hourMinuteSecond[2]),
            0);
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof Timestamp,
            "Expected Timestamp Parameter, but was " + data.getClass().getSimpleName());
        serializer.writeInt((int) ((((Timestamp) data).getTime() + offset) / 1000));
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new Timestamp(deserializer.readInt() * 1000L - offset);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Timestamp[] data = new Timestamp[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = new Timestamp(deserializer.readInt() * 1000L - offset);
        }
        return data;
    }

    public static IDataType createDateTimeType(QuotedLexer lexer, TimeZone serverZone) throws SQLException {
        if (lexer.next().type() == QuotedTokenType.OpeningRoundBracket) {
            QuotedToken token;
            Validate.isTrue((token = lexer.next()).type() == QuotedTokenType.StringLiteral);
            String timeZoneData = token.data();
            return new DataTypeDateTime("DateTime(" + timeZoneData + ")", TimeZone.getTimeZone(timeZoneData));
        }
        return new DataTypeDateTime("DateTime", serverZone);
    }
}
