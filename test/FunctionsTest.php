<?php

namespace Amp\Postgres\Test;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\Connection;
use Amp\Postgres\PostgresConfig;
use Amp\Sql\SqlException;
use function Amp\Postgres\connect;

class FunctionsTest extends AsyncTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        if (!\extension_loaded('pgsql') && !\extension_loaded('pq')) {
            $this->markTestSkipped('This test requires either ext/pgsql or pecl/pq');
        }
    }

    public function testConnect()
    {
        $connection = connect(PostgresConfig::fromString('host=localhost user=postgres password=postgres'));
        $this->assertInstanceOf(Connection::class, $connection);
    }

    public function testConnectInvalidUser()
    {
        $this->expectException(SqlException::class);

        connect(PostgresConfig::fromString('host=localhost user=invalid password=invalid'));
    }
}
