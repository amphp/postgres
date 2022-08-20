<?php

namespace Amp\Postgres\Internal;

use Amp\Future;
use Amp\Postgres\PostgresResult;

/** @internal  */
final class PgSqlResultSet implements PostgresResult, \IteratorAggregate
{
    private readonly \Traversable $iterator;

    private readonly int $rowCount;

    private readonly int $columnCount;

    /**
     * @param array<int, PgsqlType> $types
     * @param Future<PostgresResult|null> $nextResult
     */
    public function __construct(
        \PgSql\Result $handle,
        array $types,
        private readonly Future $nextResult,
    ) {
        $this->rowCount = \pg_num_rows($handle);
        $this->columnCount = \pg_num_fields($handle);

        $this->iterator = PgSqlResultIterator::iterate($handle, $types);
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function getNextResult(): ?PostgresResult
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
}
