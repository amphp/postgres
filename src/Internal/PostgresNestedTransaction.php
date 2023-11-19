<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\NestedTransaction;
use Amp\Sql\Executor;
use Amp\Sql\Transaction;

/**
 * @internal
 * @extends NestedTransaction<PostgresResult, PostgresStatement, PostgresTransaction, PostgresHandle>
 */
final class PostgresNestedTransaction extends NestedTransaction implements PostgresTransaction
{
    use PostgresTransactionDelegate;

    /**
     * @param non-empty-string $identifier
     * @param \Closure():void $release
     */
    public function __construct(
        private readonly PostgresTransaction $transaction,
        PostgresHandle $handle,
        string $identifier,
        \Closure $release,
    ) {
        parent::__construct($transaction, $handle, $identifier, $release);
    }

    /**
     * Switches return type to this library's return type.
     */
    protected function getTransaction(): PostgresTransaction
    {
        return $this->transaction;
    }

    protected function createNestedTransaction(
        Transaction $transaction,
        Executor $executor,
        string $identifier,
        \Closure $release,
    ): Transaction {
        return new PostgresNestedTransaction($transaction, $executor, $identifier, $release);
    }

    public function prepare(string $sql): PostgresStatement
    {
        $statement = parent::prepare($sql);

        // Defer statement deallocation until parent is committed or rolled back.
        $this->transaction->onClose(static fn () => $statement);

        return $statement;
    }
}
