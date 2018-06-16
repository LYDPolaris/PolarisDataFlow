package lyd.ai.native4j.jdbc.settings;

import lyd.ai.native4j.jdbc.misc.Validate;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

public class ClickHouseConfig {

    private int port;

    private String address;

    private String database;

    private String username;

    private String password;

    private int soTimeout;

    private int connectTimeout;

    private Map<SettingKey, Object> settings;


    public ClickHouseConfig(String url, Properties properties) throws SQLException {
        this.settings = parseJDBCUrl(url);
        this.settings.putAll(parseJDBCProperties(properties));

        Object obj;
        this.port = (obj = settings.remove(SettingKey.port)) == null ? 9000 : ((Integer) obj) == -1 ? 9000 : (Integer) obj;
        this.address = (obj = settings.remove(SettingKey.address)) == null ? "127.0.0.1" : String.valueOf(obj);
        this.password = (obj = settings.remove(SettingKey.password)) == null ? "lyd.ai" : String.valueOf(obj);
        this.username = (obj = settings.remove(SettingKey.user)) == null ? "default" : String.valueOf(obj);
        this.database = (obj = settings.remove(SettingKey.database)) == null ? "default" : String.valueOf(obj);
        this.soTimeout = (obj = settings.remove(SettingKey.query_timeout)) == null ? 0 : (Integer) obj;
        this.connectTimeout = (obj = settings.remove(SettingKey.connect_timeout)) == null ? 0 : (Integer) obj;
    }

    public int port() {
        return this.port;
    }

    public String address() {
        return this.address;
    }

    public String database() {
        return this.database;
    }

    public String username() {
        return this.username;
    }

    public String password() {
        return this.password;
    }

    public int queryTimeout() {
        return this.soTimeout;
    }

    public int connectTimeout() {
        return this.connectTimeout;
    }

    public Map<SettingKey, Object> settings() {
        return settings;
    }

    public Map<SettingKey, Object> parseJDBCProperties(Properties properties) {
        Map<SettingKey, Object> settings = new HashMap<SettingKey, Object>();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            for (SettingKey settingKey : SettingKey.values()) {
                String name = String.valueOf(entry.getKey());
                if (settingKey.name().equalsIgnoreCase(name)) {
                    settings.put(settingKey, settingKey.type().deserializeURL(String.valueOf(entry.getValue())));
                }
            }
        }

        return settings;
    }

    private Map<SettingKey, Object> parseJDBCUrl(String jdbcUrl) throws SQLException {
        try {
            URI uri = new URI(jdbcUrl.substring(5));
            Map<SettingKey, Object> settings = new HashMap<SettingKey, Object>();

            settings.put(SettingKey.port, uri.getPort());
            settings.put(SettingKey.address, uri.getHost());
            settings.put(SettingKey.database, uri.getPath());
            settings.putAll(extractQueryParameters(uri.getQuery()));

            return settings;
        } catch (URISyntaxException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private Map<SettingKey, Object> extractQueryParameters(String queryParameters) throws SQLException {
        Map<SettingKey, Object> parameters = new HashMap<SettingKey, Object>();
        StringTokenizer tokenizer = new StringTokenizer(queryParameters == null ? "" : queryParameters, "&");

        while (tokenizer.hasMoreTokens()) {
            String[] queryParameter = tokenizer.nextToken().split("=", 2);
            Validate.isTrue(queryParameter.length == 2,
                "ClickHouse JDBC URL Parameter '" + queryParameters + "' Error, Expected '='.");

            for (SettingKey settingKey : SettingKey.values()) {
                if (settingKey.name().equalsIgnoreCase(queryParameter[0])) {
                    parameters.put(settingKey, settingKey.type().deserializeURL(queryParameter[1]));
                }
            }
        }
        return parameters;
    }
}
