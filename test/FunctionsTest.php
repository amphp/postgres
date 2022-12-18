<?php declare(strict_types=1);

namespace Amp\Postgres\Test;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\PHPUnit\AsyncTestCase;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
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
        $this->assertInstanceOf(PostgresConnection::class, $connection);
    }

    /**
     * @depends testConnect
     */
    public function testCancelConnect()
    {
        $this->expectException(CancelledException::class);

        $deferredCancellation = new DeferredCancellation();
        $deferredCancellation->cancel();

        connect(
            PostgresConfig::fromString('host=localhost user=postgres password=postgres'),
            $deferredCancellation->getCancellation(),
        );
    }
}
