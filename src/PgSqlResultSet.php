<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\FailureException;
use Amp\Success;

final class PgSqlResultSet implements ResultSet
{
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /** @var array<int, array{string, string}> */
    private $types;

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
     * @param array<int, array{string, string}> $types
     */
    public function __construct($handle, array $types = [])
    {
        $this->handle = $handle;
        $this->types = $types;

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
    public function getCurrent(): array
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

            $result[$column] = $this->cast($this->fieldTypes[$column], $result[$column]);
        }

        return $this->currentRow = \array_combine($this->fieldNames, $result);
    }

    /**
     * @see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/catalog/pg_type.dat for OID types.
     * @see https://www.postgresql.org/docs/14/catalog-pg-type.html for pg_type catalog docs.
     *
     * @param int $oid
     * @param string $value
     *
     * @return array|bool|float|int Cast value.
     *
     * @throws ParseException
     */
    private function cast(int $oid, string $value)
    {
        [$type, $delimiter, $element] = $this->types[$oid] ?? ['S', ',', 0];

        switch ($type) {
            case 'A': // Arrays
                return $this->parser->parse($value, function (string $data) use ($element) {
                    return $this->cast($element, $data);
                }, $delimiter);

            case 'B': // Binary
                return $value === 't';

            case 'N': // Numeric
                switch ($oid) {
                    case 700: // float4
                    case 701: // float8
                    case 1700: // numeric
                        return (float) $value;

                    case 790:
                        return $value; // money includes currency symbol as string.

                    default: // Cast all other numeric types to an integer.
                        return (int) $value;
                }

            default: // Return all other types as strings.
                return $value;
        }
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
    public function getFieldCount(): int
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
    public function getFieldName(int $fieldNum): string
    {
        if (0 > $fieldNum || $this->getFieldCount() <= $fieldNum) {
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
    public function getFieldIndex(string $fieldName): int
    {
        $result = \pg_field_num($this->handle, $fieldName);

        if (-1 === $result) {
            throw new \Error(\sprintf('No field with name "%s" in result', $fieldName));
        }

        return $result;
    }
}
