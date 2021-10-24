<?php

namespace Amp\Postgres;

use Amp\AsyncGenerator;
use Amp\Promise;
use Amp\Sql\FailureException;
use Amp\Sql\Result;
use function Amp\await;

final class PgSqlResultSet implements Result, \IteratorAggregate
{
    private static Internal\ArrayParser $parser;

    private AsyncGenerator $generator;

    private int $rowCount;

    /** @var array<int, array{string, string}> */
    private $types;

    /** @var Promise<Result|null> */
    private Promise $nextResult;

    /**
     * @param resource $handle PostgreSQL result resource.
     * @param array<int, array{string, string}> $types
     */
    public function __construct($handle, Promise $nextResult)
    {
        $this->handle = $handle;
        $this->types = $types;

        $numFields = \pg_num_fields($this->handle);
        for ($i = 0; $i < $numFields; ++$i) {
            $fieldNames[] = \pg_field_name($handle, $i);
            $fieldTypes[] = \pg_field_type_oid($handle, $i);
        }

        $this->rowCount = \pg_num_rows($handle);
        $this->nextResult = $nextResult;

        $this->generator = new AsyncGenerator(static function () use ($handle, $fieldNames, $fieldTypes): \Generator {
            $position = 0;

            try {
                while (++$position <= \pg_num_rows($handle)) {
                    $result = \pg_fetch_array($handle, null, \PGSQL_NUM);

                    if ($result === false) {
                        throw new FailureException(\pg_result_error($handle));
                    }

                    yield self::processRow($fieldNames, $fieldTypes, $result);
                }
            } finally {
                \pg_free_result($handle);
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function continue(): ?array
    {
        return $this->generator->continue();
    }

    /**
     * @inheritDoc
     */
    public function dispose(): void
    {
        $this->generator->dispose();
    }

    /**
     * @inheritDoc
     */
    public function getIterator(): \Traversable
    {
        return $this->generator->getIterator();
    }

    /**
     * @inheritDoc
     */
    public function getNextResult(): ?Result
    {
        return await($this->nextResult);
    }

    /**
     * @return int Number of rows returned.
     */
    public function getRowCount(): int
    {
        return $this->rowCount;
    }

    /**
     * @param array<int, string> $fieldNames
     * @param array<int, int> $fieldTypes
     * @param array<int, mixed> $result
     *
     * @return array<string, mixed>
     * @throws ParseException
     */
    private static function processRow(array $fieldNames, array $fieldTypes, array $result): array
    {
        $columnCount = \count($result);
        for ($column = 0; $column < $columnCount; ++$column) {
            if ($result[$column] === null) {
                continue;
            }

            $result[$column] = self::cast($fieldTypes, $column, $result[$column]);
        }

        return \array_combine($fieldNames, $result);
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
    private static function cast(array $fieldTypes, int $column, string $value)
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
                    case 790: // money
                    case 1700: // numeric
                        return (float) $value;

                    default: // Cast all other numeric types to an integer.
                        return (int) $value;
                }

            default: // Return all other types as strings.
                return $value;
        }
    }
}

(function () {
    self::$parser = new Internal\ArrayParser;
})->bindTo(null, PgSqlResultSet::class)();
