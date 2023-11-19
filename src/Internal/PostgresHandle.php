<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresListener;
use Amp\Postgres\PostgresQuoter;
use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Sql\Common\NestableTransactionExecutor;

/**
 * @internal
 * @extends NestableTransactionExecutor<PostgresResult, PostgresStatement>
 */
interface PostgresHandle extends PostgresQuoter, NestableTransactionExecutor
{
    public const STATEMENT_NAME_PREFIX = "amp_";

    public function getConfig(): PostgresConfig;

    public function query(string $sql): PostgresResult;

    public function execute(string $sql, array $params = []): PostgresResult;

    public function prepare(string $sql): PostgresStatement;

    /**
     * @param non-empty-string $channel
     */
    public function notify(string $channel, string $payload = ""): PostgresResult;

    /**
     * @param non-empty-string $channel
     */
    public function listen(string $channel): PostgresListener;

    /**
     * Execute the statement with the given name and parameters.
     *
     * @param list<mixed> $params List of statement parameters, indexed starting at 0.
     */
    public function statementExecute(string $name, array $params): PostgresResult;

    /**
     * Deallocate the statement with the given name.
     */
    public function statementDeallocate(string $name): void;
}
