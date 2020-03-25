<?php

namespace Amp\Postgres;

use Amp\Sql\CommandResult;

final class PgSqlCommandResult implements CommandResult
{
    /** @var resource PostgreSQL result resource. */
    private $handle;

    /**
     * @param resource $handle PostgreSQL result resource.
     */
    public function __construct($handle)
    {
        $this->handle = $handle;
    }

    /**
     * Frees the result resource.
     */
    public function __destruct()
    {
        \pg_free_result($this->handle);
    }

    /**
     * @return int Number of rows affected by the INSERT, UPDATE, or DELETE query.
     */
    public function getAffectedRowCount(): int
    {
        return \pg_affected_rows($this->handle);
    }

    /**
     * @deprecated This is not meant to be used to get the last insertion ID. Use `INSERT ... RETURNING column_name`
     *             to get the last auto-increment ID.
     *
     * $sql = "INSERT INTO person (lastname, firstname) VALUES (?, ?) RETURNING id;"
     * $statement = yield $pool->prepare($sql);
     * $result = yield $statement->execute(['Doe', 'John']);
     * if (!yield $result->advance()) {
     *     throw new \RuntimeException("Insertion failed");
     * }
     * $id = $result->getCurrent()['id'];
     *
     * @return string
     */
    public function getLastOid(): string
    {
        return (string) \pg_last_oid($this->handle);
    }
}
