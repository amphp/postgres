<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\PooledTransaction;
use Amp\Sql\Result;
use Amp\Sql\Statement;

/**
 * @internal
 * @extends PooledTransaction<PostgresResult, PostgresStatement, PostgresTransaction>
 */
final class PostgresPooledTransaction extends PooledTransaction implements PostgresTransaction
{
    private readonly PostgresTransaction $transaction;

    protected function createStatement(Statement $statement, \Closure $release): PostgresStatement
    {
        \assert($statement instanceof PostgresStatement);
        return new PostgresPooledStatement($statement, $release);
    }

    protected function createResult(Result $result, \Closure $release): PostgresResult
    {
        \assert($result instanceof PostgresResult);
        return new PostgresPooledResult($result, $release);
    }

    /**
     * @param \Closure():void $release
     */
    public function __construct(PostgresTransaction $transaction, \Closure $release)
    {
        parent::__construct($transaction, $release);
        $this->transaction = $transaction;
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

    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        return $this->transaction->notify($channel, $payload);
    }

    public function quoteString(string $data): string
    {
        return $this->transaction->quoteString($data);
    }

    public function quoteName(string $name): string
    {
        return $this->transaction->quoteName($name);
    }
}
