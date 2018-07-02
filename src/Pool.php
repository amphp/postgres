<?php

namespace Amp\Postgres;

use Amp\Coroutine;
use Amp\Promise;
use Amp\Sql\AbstractPool;
use Amp\Sql\Connector;
use function Amp\call;

final class Pool extends AbstractPool implements Link
{
    /** @var Connection|Promise|null Connection used for notification listening. */
    private $listeningConnection;

    /** @var int Number of listeners on listening connection. */
    private $listenerCount = 0;

    /** @var bool */
    private $resetConnections = true;

    /**
     * @return Connector The Connector instance defined by the connector() function.
     */
    protected function createDefaultConnector(): Connector
    {
        return connector();
    }

    /**
     * @param bool $reset True to automatically execute RESET ALL on a connection before it is used by the pool.
     */
    public function resetConnections(bool $reset)
    {
        $this->resetConnections = $reset;
    }

    protected function pop(): \Generator
    {
        $connection = yield from parent::pop();
        \assert($connection instanceof Connection);

        if ($this->resetConnections) {
            yield $connection->query("RESET ALL");
        }

        return $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        return call(function () use ($channel, $payload) {
            $connection = yield from $this->pop();
            \assert($connection instanceof Connection);

            try {
                $result = yield $connection->notify($channel, $payload);
            } finally {
                $this->push($connection);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise
    {
        return call(function () use ($channel) {
            ++$this->listenerCount;

            if ($this->listeningConnection === null) {
                $this->listeningConnection = new Coroutine($this->pop());
            }

            if ($this->listeningConnection instanceof Promise) {
                $this->listeningConnection = yield $this->listeningConnection;
            }

            try {
                $listener = yield $this->listeningConnection->listen($channel);
                \assert($listener instanceof Listener);
            } catch (\Throwable $exception) {
                if (--$this->listenerCount === 0) {
                    $connection = $this->listeningConnection;
                    $this->listeningConnection = null;
                    $this->push($connection);
                }
                throw $exception;
            }

            $listener->onDestruct(function () {
                if (--$this->listenerCount === 0) {
                    $connection = $this->listeningConnection;
                    $this->listeningConnection = null;
                    $this->push($connection);
                }
            });

            return $listener;
        });
    }
}
