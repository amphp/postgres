<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\ConnectionTransaction;
use Amp\Sql\Common\NestableTransactionExecutor;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;

/**
 * @internal
 * @extends ConnectionTransaction<PostgresResult, PostgresStatement, PostgresTransaction, PostgresHandle>
 */
final class PostgresConnectionTransaction extends ConnectionTransaction implements PostgresTransaction
{
    use PostgresTransactionDelegate;

    public function __construct(
        private readonly PostgresHandle $handle,
        \Closure $release,
        TransactionIsolation $isolation
    ) {
        parent::__construct($handle, $release, $isolation);
    }

    protected function createNestedTransaction(
        Transaction $transaction,
        NestableTransactionExecutor $executor,
        string $identifier,
        \Closure $release,
    ): PostgresTransaction {
        \assert($executor instanceof PostgresHandle);
        return new PostgresNestedTransaction($this, $executor, $identifier, $release);
    }

    protected function getExecutor(): PostgresExecutor
    {
        return $this->handle;
    }
}
