<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\FailureException;
use Amp\Success;

final class PgSqlResultSet implements ResultSet
{
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /** @var int */
    private $position = 0;

    /** @var mixed Last row emitted. */
    private $currentRow;

    /** @var int[] */
    private $fieldTypes = [];

    /** @var string[] */
    private $fieldNames = [];

    /** @var Internal\ArrayParser */
    private $parser;

    /**
     * @param resource $handle PostgreSQL result resource.
     */
    public function __construct($handle)
    {
        $this->handle = $handle;

        $numFields = \pg_num_fields($this->handle);
        for ($i = 0; $i < $numFields; ++$i) {
            $this->fieldNames[] = \pg_field_name($this->handle, $i);
            $this->fieldTypes[] = \pg_field_type_oid($this->handle, $i);
        }

        $this->parser = new Internal\ArrayParser;
    }

    /**
     * Frees the result resource.
     */
    public function __destruct()
    {
        \pg_free_result($this->handle);
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise
    {
        $this->currentRow = null;

        if (++$this->position > \pg_num_rows($this->handle)) {
            return new Success(false);
        }

        return new Success(true);
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent(int $type = self::FETCH_ASSOC)
    {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        if ($this->position > \pg_num_rows($this->handle)) {
            throw new \Error("No more rows remain in the result set");
        }

        $result = \pg_fetch_array($this->handle, null, \PGSQL_NUM);

        if ($result === false) {
            $message = \pg_result_error($this->handle);
            \pg_free_result($this->handle);
            throw new FailureException($message);
        }

        $columnCount = \count($result);
        for ($column = 0; $column < $columnCount; ++$column) {
            if ($result[$column] === null) {
                continue;
            }

            $result[$column] = $this->cast($column, $result[$column]);
        }

        if ($type === self::FETCH_ARRAY) {
            return $this->currentRow = $result;
        }

        $assoc = [];
        foreach ($this->fieldNames as $index => $name) {
            $assoc[$name] = $result[$index];
        }

        if ($type === self::FETCH_ASSOC) {
            return $this->currentRow = $assoc;
        }

        if ($type === self::FETCH_OBJECT) {
            return $this->currentRow = (object) $assoc;
        }

        throw new \Error("Invalid result fetch type");
    }

    /**
     * @see https://github.com/postgres/postgres/blob/REL_10_STABLE/src/include/catalog/pg_type.h for OID types.
     *
     * @param int $column
     * @param string $value
     *
     * @return array|bool|float|int Cast value.
     *
     * @throws ParseException
     */
    private function cast(int $column, string $value)
    {
        switch ($this->fieldTypes[$column]) {
            case 16: // bool
                return $value === 't';

            case 20: // int8
            case 21: // int2
            case 23: // int4
            case 26: // oid
            case 27: // tid
            case 28: // xid
                return (int) $value;

            case 700: // real
            case 701: // double-precision
                return (float) $value;

            case 1000: // boolean[]
                return $this->parser->parse($value, function (string $value): bool {
                    return $value === 't';
                });

            case 1005: // int2[]
            case 1007: // int4[]
            case 1010: // tid[]
            case 1011: // xid[]
            case 1016: // int8[]
            case 1028: // oid[]
                return $this->parser->parse($value, function (string $value): int {
                    return (int) $value;
                });

            case 1021: // real[]
            case 1022: // double-precision[]
                return $this->parser->parse($value, function (string $value): float {
                    return (float) $value;
                });

            case 1020: // box[] (semi-colon delimited)
                return $this->parser->parse($value, null, ';');

            case 199:  // json[]
            case 629:  // line[]
            case 651:  // cidr[]
            case 719:  // circle[]
            case 775:  // macaddr8[]
            case 791:  // money[]
            case 1001: // bytea[]
            case 1002: // char[]
            case 1003: // name[]
            case 1006: // int2vector[]
            case 1008: // regproc[]
            case 1009: // text[]
            case 1013: // oidvector[]
            case 1014: // bpchar[]
            case 1015: // varchar[]
            case 1019: // path[]
            case 1023: // abstime[]
            case 1024: // realtime[]
            case 1025: // tinterval[]
            case 1027: // polygon[]
            case 1034: // aclitem[]
            case 1040: // macaddr[]
            case 1041: // inet[]
            case 1115: // timestamp[]
            case 1182: // date[]
            case 1183: // time[]
            case 1185: // timestampz[]
            case 1187: // interval[]
            case 1231: // numeric[]
            case 1263: // cstring[]
            case 1270: // timetz[]
            case 1561: // bit[]
            case 1563: // varbit[]
            case 2201: // refcursor[]
            case 2207: // regprocedure[]
            case 2208: // regoper[]
            case 2209: // regoperator[]
            case 2210: // regclass[]
            case 2211: // regtype[]
            case 2949: // txid_snapshot[]
            case 2951: // uuid[]
            case 3221: // pg_lsn[]
            case 3643: // tsvector[]
            case 3644: // gtsvector[]
            case 3645: // tsquery[]
            case 3735: // regconfig[]
            case 3770: // regdictionary[]
            case 3807: // jsonb[]
            case 3905: // int4range[]
            case 3907: // numrange[]
            case 3909: // tsrange[]
            case 3911: // tstzrange[]
            case 3913: // daterange[]
            case 3927: // int8range[]
            case 4090: // regnamespace[]
            case 4097: // regrole[]
                return $this->parser->parse($value);
        }

        return $value;
    }

    /**
     * @return int Number of rows in the result set.
     */
    public function numRows(): int
    {
        return \pg_num_rows($this->handle);
    }

    /**
     * @return int Number of fields in each row.
     */
    public function getNumFields(): int
    {
        return \pg_num_fields($this->handle);
    }

    /**
     * @param int $fieldNum
     *
     * @return string Column name at index $fieldNum
     *
     * @throws \Error If the field number does not exist in the result.
     */
    public function fieldName(int $fieldNum): string
    {
        if (0 > $fieldNum || $this->getNumFields() <= $fieldNum) {
            throw new \Error(\sprintf('No field with index %d in result', $fieldNum));
        }

        return \pg_field_name($this->handle, $fieldNum);
    }

    /**
     * @param string $fieldName
     *
     * @return int Index of field with given name.
     *
     * @throws \Error If the field name does not exist in the result.
     */
    public function fieldNum(string $fieldName): int
    {
        $result = \pg_field_num($this->handle, $fieldName);

        if (-1 === $result) {
            throw new \Error(\sprintf('No field with name "%s" in result', $fieldName));
        }

        return $result;
    }
}
