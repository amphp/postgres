<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Result;
use Amp\Sql\Statement;

/** @internal */
trait PostgresTransactionDelegate
{
    abstract protected function getTransaction(): PostgresTransaction;

    /**
     * @param \Closure():void $release
     */
    protected function createStatement(Statement $statement, \Closure $release): PostgresStatement
    {
        \assert($statement instanceof PostgresStatement);
        return new PostgresPooledStatement($statement, $release);
    }

    /**
     * @param \Closure():void $release
     */
    protected function createResult(Result $result, \Closure $release): PostgresResult
    {
        \assert($result instanceof PostgresResult);
        return new PostgresPooledResult($result, $release);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function query(string $sql): PostgresResult
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     */
    public function prepare(string $sql): PostgresStatement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function execute(string $sql, array $params = []): PostgresResult
    {
        return parent::execute($sql, $params);
    }

    /**
     * @param non-empty-string $channel
     */
    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        return $this->getTransaction()->notify($channel, $payload);
    }

    public function quoteString(string $data): string
    {
        return $this->getTransaction()->quoteString($data);
    }

    public function quoteName(string $name): string
    {
        return $this->getTransaction()->quoteName($name);
    }

    public function escapeByteA(string $data): string
    {
        return $this->transaction->escapeByteA($data);
    }
}
