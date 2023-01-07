<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;

abstract class AbstractQuoteTest extends AbstractConnectTest
{
    private PostgresConnection $connection;

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
