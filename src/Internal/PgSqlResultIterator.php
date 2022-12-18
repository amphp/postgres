<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\ParseException;
use Amp\Sql\SqlException;

/** @internal */
final class PgSqlResultIterator implements \IteratorAggregate
{
    /**
     * @param array<int, PgsqlType> $types
     */
    public static function iterate(\PgSql\Result $handle, array $types): \Iterator
    {
        return (new self($handle, $types))->getIterator();
    }

    /**
     * @param array<int, PgsqlType> $types
     */
    private function __construct(
        private readonly \PgSql\Result $handle,
        private readonly array $types,
    ) {
    }

    public function getIterator(): \Iterator
    {
        $fieldNames = [];
        $fieldTypes = [];
        $numFields = \pg_num_fields($this->handle);
        for ($i = 0; $i < $numFields; ++$i) {
            $fieldNames[] = \pg_field_name($this->handle, $i);
            $fieldTypes[] = \pg_field_type_oid($this->handle, $i);
        }

        $position = 0;

        try {
            while (++$position <= \pg_num_rows($this->handle)) {
                /** @var list<string|null>|false $result */
                $result = \pg_fetch_array($this->handle, null, \PGSQL_NUM);

                if ($result === false) {
                    throw new SqlException(\pg_result_error($this->handle));
                }

                /** @var list<int> $fieldTypes */
                yield \array_combine($fieldNames, \array_map($this->cast(...), $fieldTypes, $result));
            }
        } finally {
            \pg_free_result($this->handle);
        }
    }

    /**
     * @see https://github.com/postgres/postgres/blob/REL_14_STABLE/src/include/catalog/pg_type.dat for OID types.
     * @see https://www.postgresql.org/docs/14/catalog-pg-type.html for pg_type catalog docs.
     *
     * @throws ParseException
     */
    private function cast(int $oid, ?string $value): string|int|bool|array|float|null
    {
        if ($value === null) {
            return null;
        }

        $type = $this->types[$oid] ?? PgsqlType::getDefaultType();

        return match ($type->type) {
            'A' => ArrayParser::parse( // Array
                $value,
                fn (string $data) => $this->cast($type->element, $data),
                $type->delimiter,
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
