<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\Common\NestableTransaction;
use Amp\Sql\Transaction;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;

/**
 * @extends NestableTransaction<PostgresResult, PostgresStatement, PostgresTransaction>
 */
final class PostgresNestableTransaction extends NestableTransaction implements PostgresLink
{
    protected function createNestedTransaction(
        Transaction $transaction,
        \Closure $release,
        string $identifier,
    ): Transaction {
        return new Internal\PostgresNestedTransaction($transaction, $release, $identifier);
    }

    /**
     * Changes return type to this library's Transaction type.
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): PostgresTransaction {
        return parent::beginTransaction($isolation);
    }

    public function quoteString(string $data): string
    {
        return $this->transaction->quoteString($data);
    }

    public function quoteName(string $name): string
    {
        return $this->transaction->quoteName($name);
    }

    public function escapeByteA(string $data): string
    {
        return $this->transaction->escapeByteA($data);
    }
}
