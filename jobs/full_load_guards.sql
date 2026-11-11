-- Included by build_full_load_sql.py in the publish deployment script.
CREATE OR REPLACE FUNCTION hdg_load.checked_text(v text, n integer, byte_semantics boolean)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $f$
BEGIN
    IF v = '' OR char_length(v)>n OR (byte_semantics AND octet_length(v)>n) THEN
        RAISE EXCEPTION 'String is empty or exceeds declared Oracle length % (byte=%)', n, byte_semantics;
    END IF;
    RETURN v;
END $f$;

CREATE OR REPLACE FUNCTION hdg_load.checked_numeric(v text, p integer, s integer)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $f$
DECLARE n numeric;
BEGIN
    IF v IS NULL THEN RETURN NULL; END IF;
    IF v !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$' THEN
        RAISE EXCEPTION 'Invalid finite numeric transport value';
    END IF;
    n := v::numeric;
    IF p IS NOT NULL AND (n <> round(n,s) OR abs(n) >= power(10::numeric,p-s)) THEN
        RAISE EXCEPTION 'Numeric cannot fit NUMBER(%,%) without rounding', p, s;
    END IF;
    RETURN n;
END $f$;

CREATE OR REPLACE FUNCTION hdg_load.checked_timestamp(v text, p integer)
RETURNS timestamp without time zone LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path=pg_catalog SET datestyle='ISO, YMD' AS $f$
DECLARE n timestamp without time zone; fmt text; pattern text;
BEGIN
    IF p NOT BETWEEN 0 AND 6 THEN RAISE EXCEPTION 'Unsupported timestamp precision'; END IF;
    pattern := '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' ||
        CASE WHEN p=0 THEN '' ELSE '[.][0-9]{' || p || '}' END || ' (AD|BC)$';
    IF v !~ pattern THEN RAISE EXCEPTION 'Non-canonical timestamp transport'; END IF;
    n := v::timestamp without time zone;
    fmt := 'YYYY-MM-DD HH24:MI:SS' || CASE WHEN p=0 THEN '' ELSE '.FF' || p END || ' BC';
    IF to_char(n,fmt) IS DISTINCT FROM v THEN
        RAISE EXCEPTION 'Timestamp round trip differs';
    END IF;
    RETURN n;
END $f$;
