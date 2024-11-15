<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Postgres\PostgresParseException;
use Amp\Postgres\PostgresResult;
use Amp\Sql\SqlException;

/**
 * @internal
 * @psalm-import-type TRowType from PostgresResult
 */
final class PgSqlResultIterator
{
    use ForbidCloning;
    use ForbidSerialization;

    /**
     * @param array<int, PgSqlType> $types
     *
     * @return \Iterator<int, TRowType>
     */
    public static function iterate(\PgSql\Result $handle, array $types): \Iterator
    {
        return (new self($handle, $types))->getIterator();
    }

    /**
     * @param array<int, PgSqlType> $types
     */
    private function __construct(
        private readonly \PgSql\Result $handle,
        private readonly array $types,
    ) {
    }

    private function getIterator(): \Iterator
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
                $result = \pg_fetch_array($this->handle, mode: \PGSQL_NUM);

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
     * @return list<mixed>|bool|int|float|string|null
     *
     * @throws PostgresParseException
     */
    private function cast(int $oid, ?string $value): array|bool|int|float|string|null
    {
        if ($value === null) {
            return null;
        }

        $type = $this->types[$oid] ?? PgSqlType::getDefaultType();

        return match ($type->category) {
            'A' => match ($type->name) { // Array
                'int2vector', 'oidvector' => $value, // Deprecated array types
                default => ArrayParser::parse(
                    $value,
                    fn (string $data) => $this->cast($type->element, $data),
                    $type->delimiter,
                ),
            },
            'B' => match ($value) {
                't' => true,
                'f' => false,
                default => throw new PostgresParseException('Unexpected value for boolean field: ' . $value),
            }, // Boolean
            'N' => match ($type->name) { // Numeric
                'float4', 'float8' => (float) $value,
                'int2', 'int4', 'oid' => (int) $value,
                'int8' => \PHP_INT_SIZE >= 8 ? (int) $value : $value, // String on 32-bit systems
                default => $value, // Return a string for all other numeric types
            },
            'U' => match ($type->name) {
                'bytea' => \pg_unescape_bytea($value),
                default => $value,
            },
            default => $value, // Return a string for all other types
        };
    }
}
