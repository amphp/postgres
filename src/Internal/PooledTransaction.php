<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\Result;
use Amp\Sql\Statement as SqlStatement;

/** @internal  */
final class PooledTransaction extends SqlPooledTransaction implements PostgresTransaction
{
    private readonly PostgresTransaction $transaction;

    protected function createStatement(SqlStatement $statement, \Closure $release): SqlStatement
    {
        return new PooledStatement($statement, $release);
    }

    /**
     * @param \Closure():void $release
     */
    public function __construct(PostgresTransaction $transaction, \Closure $release)
    {
        parent::__construct($transaction, $release);
        $this->transaction = $transaction;
    }

    public function notify(string $channel, string $payload = ""): Result
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
