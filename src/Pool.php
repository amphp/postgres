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

    protected function createDefaultConnector(): Connector
    {
        return connector();
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        return call(function () use ($channel, $payload) {
            /** @var Connection $connection */
            $connection = yield from $this->pop();

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
                /** @var Listener $listener */
                $listener = yield $this->listeningConnection->listen($channel);
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
