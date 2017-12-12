<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Success;

class PgSqlResultSet implements ResultSet {
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /** @var int */
    private $position = 0;

    /** @var mixed Last row emitted. */
    private $currentRow;

    /** @var int Next row fetch type. */
    private $type = self::FETCH_ASSOC;

    /**
     * @param resource $handle PostgreSQL result resource.
     */
    public function __construct($handle) {
        $this->handle = $handle;
    }

    /**
     * Frees the result resource.
     */
    public function __destruct() {
        \pg_free_result($this->handle);
    }

    /**
     * {@inheritdoc}
     */
    public function advance(int $type = self::FETCH_ASSOC): Promise {
        $this->currentRow = null;
        $this->type = $type;

        if (++$this->position > \pg_num_rows($this->handle)) {
            return new Success(false);
        }

        return new Success(true);
    }

    /**
     * {@inheritdoc}
     */
    public function getCurrent() {
        if ($this->currentRow !== null) {
            return $this->currentRow;
        }

        if ($this->position > \pg_num_rows($this->handle)) {
            throw new \Error("No more rows remain in the result set");
        }

        switch ($this->type) {
            case self::FETCH_ASSOC:
                $result = \pg_fetch_array($this->handle, null, \PGSQL_ASSOC);
                break;
            case self::FETCH_ARRAY:
                $result = \pg_fetch_array($this->handle, null, \PGSQL_NUM);
                break;
            case self::FETCH_OBJECT:
                $result = \pg_fetch_object($this->handle);
                break;
            default:
                throw new \Error("Invalid result fetch type");
        }

        if ($result === false) {
            $message = \pg_result_error($this->handle);
            \pg_free_result($this->handle);
            throw new FailureException($message);
        }

        return $this->currentRow = $result;
    }

    /**
     * @return int Number of rows in the result set.
     */
    public function numRows(): int {
        return \pg_num_rows($this->handle);
    }

    /**
     * @return int Number of fields in each row.
     */
    public function numFields(): int {
        return \pg_num_fields($this->handle);
    }

    /**
     * @param int $fieldNum
     *
     * @return string Column name at index $fieldNum
     *
     * @throws \Error If the field number does not exist in the result.
     */
    public function fieldName(int $fieldNum): string {
        return \pg_field_name($this->handle, $this->filterNameOrNum($fieldNum));
    }

    /**
     * @param string $fieldName
     *
     * @return int Index of field with given name.
     *
     * @throws \Error If the field name does not exist in the result.
     */
    public function fieldNum(string $fieldName): int {
        $result = \pg_field_num($this->handle, $fieldName);

        if (-1 === $result) {
            throw new \Error(\sprintf('No field with name "%s" in result', $fieldName));
        }

        return $result;
    }

    /**
     * @param int|string $fieldNameOrNum Field name or index.
     *
     * @return string Name of the field type.
     *
     * @throws \Error If the field number does not exist in the result.
     */
    public function fieldType($fieldNameOrNum): string {
        return \pg_field_type($this->handle, $this->filterNameOrNum($fieldNameOrNum));
    }

    /**
     * @param int|string $fieldNameOrNum Field name or index.
     *
     * @return int Storage required for field. -1 indicates a variable length field.
     *
     * @throws \Error If the field number does not exist in the result.
     */
    public function fieldSize($fieldNameOrNum): int {
        return \pg_field_size($this->handle, $this->filterNameOrNum($fieldNameOrNum));
    }

    /**
     * @param int|string $fieldNameOrNum Field name or index.
     *
     * @return int Field index.
     *
     * @throws \Error
     */
    private function filterNameOrNum($fieldNameOrNum): int {
        if (\is_string($fieldNameOrNum)) {
            return $this->fieldNum($fieldNameOrNum);
        }

        if (!\is_int($fieldNameOrNum)) {
            throw new \Error('Must provide a string name or integer field number');
        }

        if (0 > $fieldNameOrNum || $this->numFields() <= $fieldNameOrNum) {
            throw new \Error(\sprintf('No field with index %d in result', $fieldNameOrNum));
        }

        return $fieldNameOrNum;
    }
}
