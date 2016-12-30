<?php

namespace Amp\Postgres;

use Interop\Async\Promise;

class ConnectionPool extends AbstractPool {
    const DEFAULT_MAX_CONNECTIONS = 100;
    const DEFAULT_CONNECT_TIMEOUT = 5000;

    /** @var string */
    private $connectionString;

    /** @var int */
    private $connectTimeout;

    /** @var int */
    private $maxConnections;

    /**
     * @param string $connectionString
     * @param int $maxConnections
     * @param int $connectTimeout
     */
    public function __construct(
        string $connectionString,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT
    ) {
        parent::__construct();

        $this->connectionString = $connectionString;
        $this->connectTimeout = $connectTimeout;

        $this->maxConnections = $maxConnections;
        if (1 > $this->maxConnections) {
            $this->maxConnections = 1;
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function createConnection(): Promise {
        return connect($this->connectionString, $this->connectTimeout);
    }

    /**
     * {@inheritdoc}
     */
    public function getMaxConnections(): int {
        return $this->maxConnections;
    }
}
