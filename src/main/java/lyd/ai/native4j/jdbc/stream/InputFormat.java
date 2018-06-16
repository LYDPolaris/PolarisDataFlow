package lyd.ai.native4j.jdbc.stream;

import lyd.ai.native4j.jdbc.data.Block;

import java.sql.SQLException;

public interface InputFormat {
    Block next(Block header, int maxRows) throws SQLException;
}
