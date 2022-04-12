<?php

namespace Amp\Postgres\Test;

use Amp\Postgres\PostgresConfig;
use PHPUnit\Framework\TestCase;

class ConnectionConfigTest extends TestCase
{
    public function testBasicSyntax(): void
    {
        $config = PostgresConfig::fromString("host=localhost port=5434 user=postgres password=test db=test");

        $this->assertSame($config->getHost(), "localhost");
        $this->assertSame($config->getPort(), 5434);
        $this->assertSame($config->getUser(), "postgres");
        $this->assertSame($config->getPassword(), "test");
        $this->assertSame($config->getDatabase(), "test");
    }

    public function testAlternativeSyntax(): void
    {
        $config = PostgresConfig::fromString("host=localhost;port=5434;user=postgres;password=test;db=test");

        $this->assertSame($config->getHost(), "localhost");
        $this->assertSame($config->getPort(), 5434);
        $this->assertSame($config->getUser(), "postgres");
        $this->assertSame($config->getPassword(), "test");
        $this->assertSame($config->getDatabase(), "test");
    }

    public function testNoHost(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Host must be provided in connection string");
        $config = PostgresConfig::fromString("user=postgres");
    }

    public function testInvalidString(): void
    {
        $this->expectException(\Error::class);
        $this->expectExceptionMessage("Host must be provided in connection string");
        $config = PostgresConfig::fromString("invalid connection string");
    }

    public function testSslMode(): void
    {
        $config = PostgresConfig::fromString("host=localhost sslmode=verify-ca");
        $this->assertSame('verify-ca', $config->getSslMode());

        $altered = $config->withoutSslMode();
        $this->assertNull($altered->getSslMode());
        $this->assertSame('verify-ca', $config->getSslMode());

        $altered = $altered->withSslMode('allow');
        $this->assertSame('allow', $altered->getSslMode());

        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Invalid SSL mode');

        $config->withSslMode('invalid');
    }
}
