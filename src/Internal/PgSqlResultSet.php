<?php

namespace Amp\Postgres\Internal;

use Amp\Future;
use Amp\Postgres\Internal;
use Amp\Postgres\ParseException;
use Amp\Sql\Result;
use Amp\Sql\SqlException;

/** @internal  */
final class PgSqlResultSet implements Result, \IteratorAggregate
{
    private static Internal\ArrayParser $parser;

    private readonly \Generator $iterator;

    private readonly int $rowCount;

    private readonly int $columnCount;

    /**
     * @param array<int, array{string, string, int}> $types
     * @param Future<Result|null> $nextResult
     */
    public function __construct(
        \PgSql\Result $handle,
        array $types,
        private readonly Future $nextResult,
    ) {
        /** @psalm-suppress RedundantPropertyInitializationCheck */
        self::$parser ??= new Internal\ArrayParser;

        $this->rowCount = \pg_num_rows($handle);
        $this->columnCount = \pg_num_fields($handle);

        $this->iterator = self::generate($handle, $types);
    }

    private static function generate(\PgSql\Result $handle, array $types): \Generator
    {
        $fieldNames = [];
        $fieldTypes = [];
        $numFields = \pg_num_fields($handle);
        for ($i = 0; $i < $numFields; ++$i) {
            $fieldNames[] = \pg_field_name($handle, $i);
            $fieldTypes[] = \pg_field_type_oid($handle, $i);
        }

        $position = 0;

        try {
            while (++$position <= \pg_num_rows($handle)) {
                /** @var list<string|null>|false $result */
                $result = \pg_fetch_array($handle, null, \PGSQL_NUM);

                if ($result === false) {
                    throw new SqlException(\pg_result_error($handle));
                }

                /** @var list<int> $fieldTypes */
                yield self::processRow($types, $fieldNames, $fieldTypes, $result);
            }
        } finally {
            \pg_free_result($handle);
        }
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function getNextResult(): ?Result
    {
        return $this->nextResult->await();
    }

    /**
     * @return int Number of rows returned.
     */
    public function getRowCount(): int
    {
        return $this->rowCount;
    }

    /**
     * @return int Number of columns returned.
     */
    public function getColumnCount(): int
    {
        return $this->columnCount;
    }

    /**
     * @param array<int, array{string, string, int}> $types
     * @param list<string> $fieldNames
     * @param list<int> $fieldTypes
     * @param list<string|null> $result
     *
     * @return array<string, mixed>
     * @throws ParseException
     */
    private static function processRow(array $types, array $fieldNames, array $fieldTypes, array $result): array
    {
        $columnCount = \count($result);
        for ($column = 0; $column < $columnCount; ++$column) {
            if ($result[$column] === null) {
                continue;
            }

            /** @psalm-suppress InvalidArgument $result[$column] will be a string when passed to {@see cast()} */
            $result[$column] = self::cast($types, $fieldTypes[$column], $result[$column]);
        }

        return \array_combine($fieldNames, $result);
    }

    /**
     * @see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/catalog/pg_type.dat for OID types.
     * @see https://www.postgresql.org/docs/14/catalog-pg-type.html for pg_type catalog docs.
     *
     * @param array<int, array{string, string, int}> $types
     *
     * @return string|int|bool|array|float Cast value.
     *
     * @throws ParseException
     */
    private static function cast(array $types, int $oid, string $value): string|int|bool|array|float
    {
        [$type, $delimiter, $element] = $types[$oid] ?? ['S', ',', 0];

        return match ($type) {
            'A' => self::$parser->parse( // Array
                $value,
                static fn (string $data) => self::cast($types, $element, $data),
                $delimiter,
            ),
            'B' => $value === 't', // Boolean
            'N' => match ($oid) { // Numeric
                700, 701, 790, 1700 => (float) $value, // float4, float8, money, and numeric to float
                default => (int) $value, // All other numeric types cast to an integer
            },
            default => $value, // Return a string for all other types
        };
    }
}
