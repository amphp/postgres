<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Pipeline;
use Amp\Sql\Result;
use Amp\Sql\SqlException;

final class PgSqlResultSet implements Result, \IteratorAggregate
{
    private static Internal\ArrayParser $parser;

    private readonly ConcurrentIterator $generator;

    private readonly int $rowCount;

    private readonly int $columnCount;

    /** @var Future<Result|null> */
    private readonly Future $nextResult;

    /**
     * @param \PgSql\Result $handle PostgreSQL result resource.
     * @param array<int, array{string, string}> $types
     * @param Future<Result|null> $nextResult
     */
    public function __construct(\PgSql\Result $handle, array $types, Future $nextResult)
    {
        /** @psalm-suppress RedundantPropertyInitializationCheck */
        if (!isset(self::$parser)) {
            self::$parser = new Internal\ArrayParser;
        }

        $fieldNames = [];
        $fieldTypes = [];
        $numFields = \pg_num_fields($handle);
        for ($i = 0; $i < $numFields; ++$i) {
            $fieldNames[] = \pg_field_name($handle, $i);
            $fieldTypes[] = \pg_field_type_oid($handle, $i);
        }

        $this->rowCount = \pg_num_rows($handle);
        $this->columnCount = \pg_num_fields($handle);
        $this->nextResult = $nextResult;

        /** @var list<int> $fieldTypes */
        $this->generator = Pipeline::fromIterable(static function () use (
            $handle,
            $types,
            $fieldNames,
            $fieldTypes
        ): \Generator {
            $position = 0;

            try {
                while (++$position <= \pg_num_rows($handle)) {
                    $result = \pg_fetch_array($handle, null, \PGSQL_NUM);

                    if ($result === false) {
                        throw new SqlException(\pg_result_error($handle));
                    }

                    yield self::processRow($types, $fieldNames, $fieldTypes, $result);
                }
            } finally {
                \pg_free_result($handle);
            }
        })->getIterator();
    }

    public function getIterator(): \Traversable
    {
        return $this->generator->getIterator();
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
     * @param array<int, array{string, string}> $types
     * @param array<int, string> $fieldNames
     * @param array<int, int> $fieldTypes
     * @param array<int, mixed> $result
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

            $result[$column] = self::cast($types, $fieldTypes[$column], $result[$column]);
        }

        return \array_combine($fieldNames, $result);
    }

    /**
     * @see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/catalog/pg_type.dat for OID types.
     * @see https://www.postgresql.org/docs/14/catalog-pg-type.html for pg_type catalog docs.
     *
     * @param array<int, array{string, string}> $types
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
