<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\PooledTransaction;
use Amp\Sql\Transaction;

/**
 * @internal
 * @extends PooledTransaction<PostgresResult, PostgresStatement, PostgresTransaction>
 */
final class PostgresPooledTransaction extends PooledTransaction implements PostgresTransaction
{
    use PostgresTransactionDelegate;

    /**
     * @param \Closure():void $release
     */
    public function __construct(private readonly PostgresTransaction $transaction, \Closure $release)
    {
        parent::__construct($transaction, $release);
    }

    protected function getExecutor(): PostgresExecutor
    {
        return $this->transaction;
    }

    protected function createTransaction(Transaction $transaction, \Closure $release): PostgresTransaction
    {
        \assert($transaction instanceof PostgresTransaction);
        return new PostgresPooledTransaction($transaction, $release);
    }
}
