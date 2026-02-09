package com.ctbcins.hrimport.util;

public final class SqlIdentifierValidator {
    private SqlIdentifierValidator() {}

    /**
     * Very small whitelist check for SQL identifiers (table/column names).
     * Allows simple unquoted identifiers: letters, digits, underscore, starting with a letter or underscore.
     * Does NOT allow quotes, spaces, punctuation, or SQL keywords. For quoted identifiers, caller should
     * provide the exact quoted string (e.g. "MyTable").
     */
    public static boolean isValidSimpleIdentifier(String id) {
        if (id == null || id.isEmpty()) return false;
        return id.matches("[A-Za-z_][A-Za-z0-9_]*");
    }

    public static String requireValidSimpleIdentifier(String id) {
        if (!isValidSimpleIdentifier(id)) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + id);
        }
        return id;
    }
}

