package com.ctbcins.hrimport.util;

public final class SqlUtils {
    private SqlUtils() {}

    public static String quoteTable(String schema, String table) {
        SqlIdentifierValidator.requireValidSimpleIdentifier(schema);
        SqlIdentifierValidator.requireValidSimpleIdentifier(table);
        return String.format("%s.\"%s\"", schema, table);
    }

    public static String quoteIdentifier(String identifier) {
        SqlIdentifierValidator.requireValidSimpleIdentifier(identifier);
        return String.format("\"%s\"", identifier);
    }
}

