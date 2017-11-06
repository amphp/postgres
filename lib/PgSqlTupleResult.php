<?php

namespace Amp\Postgres;

use Amp\Producer;

class PgSqlTupleResult extends TupleResult {
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /**
     * @param resource $handle PostgreSQL result resource.
     */
    public function __construct($handle) {
        $this->handle = $handle;
        parent::__construct(new Producer(static function (callable $emit) use ($handle) {
            $count = \pg_num_rows($handle);
            for ($i = 0; $i < $count; ++$i) {
                if (!\is_resource($handle)) {
                    return; // Result object discarded, simply return.
                }

                $result = \pg_fetch_assoc($handle);
                if ($result === false) {
                    throw new FailureException(\pg_result_error($handle));
                }
                yield $emit($result);
            }
        }));
    }

    /**
     * Frees the result resource.
     */
    public function __destruct() {
        \pg_free_result($this->handle);
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
