<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\Postgres\Internal\PostgresHandleConnection;
use Amp\Postgres\PostgresConfig;

abstract class AbstractQuoteTest extends AbstractConnectTest
{
    private PostgresHandleConnection $connection;

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->connect(PostgresConfig::fromString('host=localhost user=postgres password=postgres'));
    }

    public function tearDown(): void
    {
        parent::tearDown();
        $this->connection->close();
    }

    public function testEscapeByteA(): void
    {
        $this->assertSame('\x00', $this->connection->escapeByteA("\0"));
    }

    public function testQuoteString(): void
    {
        $this->assertSame("'\"''test''\"'", $this->connection->quoteString("\"'test'\""));
    }

    public function testQuoteName(): void
    {
        $this->assertSame("\"\"\"'test'\"\"\"", $this->connection->quoteName("\"'test'\""));
    }
}
