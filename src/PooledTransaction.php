<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Common\PooledTransaction as SqlPooledTransaction;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement as SqlStatement;

final class PooledTransaction extends SqlPooledTransaction implements Transaction
{
    /** @var Transaction|null */
    private $transaction;

    protected function createStatement(SqlStatement $statement, callable $release): SqlStatement
    {
        return new PooledStatement($statement, $release);
    }

    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }

    /**
     * @param Transaction $transaction
     * @param callable    $release
     */
    public function __construct(Transaction $transaction, callable $release)
    {
        parent::__construct($transaction, $release);
        $this->transaction = $transaction;
    }

    public function notify(string $channel, string $payload = ""): Promise
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
