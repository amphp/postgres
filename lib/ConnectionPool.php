<?php

namespace Amp\Postgres;

use Amp\Promise;

class ConnectionPool extends AbstractPool {
    const DEFAULT_MAX_CONNECTIONS = 100;

    /** @var string */
    private $connectionString;

    /** @var int */
    private $maxConnections;

    /**
     * @param string $connectionString
     * @param int $maxConnections
     */
    public function __construct(string $connectionString, int $maxConnections = self::DEFAULT_MAX_CONNECTIONS) {
        parent::__construct();

        $this->connectionString = $connectionString;

        $this->maxConnections = $maxConnections;
        if (1 > $this->maxConnections) {
            $this->maxConnections = 1;
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function createConnection(): Promise {
        return connect($this->connectionString);
    }

    /**
     * {@inheritdoc}
     */
    public function getMaxConnections(): int {
        return $this->maxConnections;
    }
}
