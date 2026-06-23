<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Cancellation;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Internal\PostgresHandle;
use Amp\Postgres\Internal\PostgresHandleConnection;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresListener;
use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Sql\SqlConnectionException;
use Revolt\EventLoop;
use function Amp\delay;

final class TransactionReleaseRaceTest extends AsyncTestCase
{
    public function testSequentialTransactionsWaitForPreviousRelease(): void
    {
        $connection = self::createConnection();

        $transaction = $connection->beginTransaction();
        $transaction->commit();

        $transaction = $connection->beginTransaction();
        $transaction->commit();

        delay(0);

        $this->expectNotToPerformAssertions();
    }

    private static function createConnection(): PostgresConnection
    {
        $reflection = new \ReflectionClass(RaceTestConnection::class);
        $connection = $reflection->newInstanceWithoutConstructor();
        $reflection->getConstructor()->invoke($connection, new RaceTestHandle());

        return $connection;
    }
}

final class RaceTestConnection extends PostgresHandleConnection
{
    public static function connect(PostgresConfig $config, ?Cancellation $cancellation = null): self
    {
        throw new SqlConnectionException('Not used in this test');
    }
}

final class RaceTestHandle implements PostgresHandle
{
    public function close(): void
    {
    }

    public function commit(): void
    {
    }

    public function createSavepoint(string $identifier): void
    {
    }

    public function escapeByteA(string $data): string
    {
        return $data;
    }

    public function execute(string $sql, array $params = []): PostgresResult
    {
        return new RaceTestResult();
    }

    public function getConfig(): PostgresConfig
    {
        return new PostgresConfig(host: 'localhost');
    }

    public function getLastUsedAt(): int
    {
        return 0;
    }

    public function isClosed(): bool
    {
        return false;
    }

    public function listen(string $channel): PostgresListener
    {
        throw new \BadMethodCallException('Not used in this test');
    }

    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        return new RaceTestResult();
    }

    public function onClose(\Closure $onClose): void
    {
        EventLoop::queue($onClose);
    }

    public function prepare(string $sql): PostgresStatement
    {
        throw new \BadMethodCallException('Not used in this test');
    }

    public function query(string $sql): PostgresResult
    {
        return new RaceTestResult();
    }

    public function quoteIdentifier(string $name): string
    {
        return $name;
    }

    public function quoteLiteral(string $data): string
    {
        return $data;
    }

    public function releaseSavepoint(string $identifier): void
    {
    }

    public function rollback(): void
    {
    }

    public function rollbackTo(string $identifier): void
    {
    }

    public function statementDeallocate(string $name): void
    {
    }

    public function statementExecute(string $name, array $params): PostgresResult
    {
        return new RaceTestResult();
    }
}

final class RaceTestResult implements PostgresResult, \IteratorAggregate
{
    public function fetchRow(): ?array
    {
        return null;
    }

    public function getColumnCount(): ?int
    {
        return null;
    }

    public function getIterator(): \Traversable
    {
        return new \EmptyIterator();
    }

    public function getNextResult(): ?PostgresResult
    {
        return null;
    }

    public function getRowCount(): ?int
    {
        return null;
    }
}
