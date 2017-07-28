<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Emitter;
use Amp\Loop;
use Amp\Promise;
use Amp\Success;
use pq;
use function Amp\call;
use function Amp\coroutine;

class PqExecutor implements Executor {
    use CallableMaker;

    /** @var \pq\Connection PostgreSQL connection object. */
    private $handle;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var \Amp\Deferred|null */
    private $busy;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var \Amp\Emitter[] */
    private $listeners;

    /** @var \Amp\Postgres\Internal\PqStatementStorage[] */
    private $statements = [];

    /** @var callable */
    private $send;

    /** @var callable */
    private $fetch;

    /** @var callable */
    private $unlisten;

    /** @var callable */
    private $release;

    /** @var callable */
    private $deallocate;

    /**
     * Connection constructor.
     *
     * @param \pq\Connection $handle
     */
    public function __construct(pq\Connection $handle) {
        $this->handle = $handle;

        $deferred = &$this->deferred;
        $listeners = &$this->listeners;

        $this->poll = Loop::onReadable($this->handle->socket, static function ($watcher) use (&$deferred, &$listeners, $handle) {
            $status = $handle->poll();

            if ($deferred === null) {
                return; // No active query, only notification listeners.
            }

            if ($status === pq\Connection::POLLING_FAILED) {
                $deferred->fail(new FailureException($handle->errorMessage));
            } elseif (!$handle->busy) {
                $deferred->resolve($handle->getResult());
            }

            if (!$deferred && !$handle->busy && empty($listeners)) {
                Loop::disable($watcher);
            }
        });

        $this->await = Loop::onWritable($this->handle->socket, static function ($watcher) use (&$deferred, $handle) {
            if (!$handle->flush()) {
                return; // Not finished sending data, continue polling for writability.
            }

            Loop::disable($watcher);
        });

        Loop::disable($this->poll);
        Loop::disable($this->await);

        $this->send = coroutine($this->callableFromInstanceMethod("send"));
        $this->fetch = coroutine($this->callableFromInstanceMethod("fetch"));
        $this->unlisten = $this->callableFromInstanceMethod("unlisten");
        $this->release = $this->callableFromInstanceMethod("release");
        $this->deallocate = $this->callableFromInstanceMethod("deallocate");
    }

    /**
     * Frees Io watchers from loop.
     */
    public function __destruct() {
        Loop::cancel($this->poll);
        Loop::cancel($this->await);
    }

    /**
     * @param callable $method Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Generator
     *
     * @resolve resource
     *
     * @throws \Amp\Postgres\FailureException
     */
    private function send(callable $method, ...$args): \Generator {
        while ($this->busy) {
            try {
                yield $this->busy->promise();
            } catch (\Throwable $exception) {
                // Ignore failure from another operation.
            }
        }

        try {
            $handle = $method(...$args);

            $this->deferred = $this->busy = new Deferred;

            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }

            try {
                $result = yield $this->deferred->promise();
            } finally {
                $this->deferred = null;
            }

            if (!$result instanceof pq\Result) {
                throw new FailureException("Unknown query result");
            }
        } catch (pq\Exception $exception) {
            throw new FailureException($this->handle->errorMessage, 0, $exception);
        } finally {
            $this->busy = null;
        }

        switch ($result->status) {
            case pq\Result::EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case pq\Result::COMMAND_OK:
                if ($handle instanceof pq\Statement) {
                    return $handle; // Will be wrapped into a PqStatement object.
                }

                return new PqCommandResult($result);

            case pq\Result::TUPLES_OK:
                return new PqBufferedResult($result);

            case pq\Result::SINGLE_TUPLE:
                $this->busy = new Deferred;
                $result = new PqUnbufferedResult($this->fetch, $result);
                $result->onComplete($this->release);
                return $result;

            case pq\Result::NONFATAL_ERROR:
            case pq\Result::FATAL_ERROR:
                throw new QueryError($result->errorMessage);

            case pq\Result::BAD_RESPONSE:
                throw new FailureException($result->errorMessage);

            default:
                throw new FailureException("Unknown result status");
        }
    }

    private function fetch(): \Generator {
        if (!$this->handle->busy) { // Results buffered.
            $result = $this->handle->getResult();
        } else {
            $this->deferred = new Deferred;

            Loop::enable($this->poll);

            try {
                $result = yield $this->deferred->promise();
            } finally {
                $this->deferred = null;
            }
        }

        switch ($result->status) {
            case pq\Result::TUPLES_OK: // End of result set.
                return null;

            case pq\Result::SINGLE_TUPLE:
                return $result;

            default:
                throw new FailureException($result->errorMessage);
        }
    }

    private function release() {
        $deferred = $this->busy;
        $this->busy = null;
        $deferred->resolve();
    }

    private function deallocate(string $name) {
        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->count) {
            return;
        }

        unset($this->statements[$name]);

        Promise\rethrow(new Coroutine($this->send([$storage->statement, "deallocateAsync"])));
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return new Coroutine($this->send([$this->handle, "execAsync"], $sql));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Promise {
        return new Coroutine($this->send([$this->handle, "execParamsAsync"], $sql, $params));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        $name = self::STATEMENT_NAME_PREFIX . \sha1($sql);

        if (isset($this->statements[$name])) {
            $storage = $this->statements[$name];
            ++$storage->count;

            if ($storage->promise) {
                return $storage->promise;
            }

            return new Success(new PqStatement($storage->statement, $name, $this->send, $this->deallocate));
        }

        $this->statements[$name] = $storage = new Internal\PqStatementStorage;

        $storage->promise = call(function () use ($storage, $name, $sql) {
            $statement = yield from $this->send([$this->handle, "prepareAsync"], $name, $sql);
            $storage->statement = $statement;
            return new PqStatement($statement, $name, $this->send, $this->deallocate);
        });
        $storage->promise->onResolve(function () use ($storage) {
            $storage->promise = null;
        });
        return $storage->promise;
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return new Coroutine($this->send([$this->handle, "notifyAsync"], $channel, $payload));
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise {
        return call(function () use ($channel) {
            if (isset($this->listeners[$channel])) {
                throw new QueryError(\sprintf("Already listening on channel '%s'", $channel));
            }

            $this->listeners[$channel] = $emitter = new Emitter;

            try {
                yield from $this->send(
                    [$this->handle, "listenAsync"],
                    $channel,
                    static function (string $channel, string $message, int $pid) use ($emitter) {
                        $notification = new Notification;
                        $notification->channel = $channel;
                        $notification->pid = $pid;
                        $notification->payload = $message;
                        $emitter->emit($notification);
                    }
                );
            } catch (\Throwable $exception) {
                unset($this->listeners[$channel]);
                throw $exception;
            }

            Loop::enable($this->poll);
            return new Listener($emitter->iterate(), $channel, $this->unlisten);
        });
    }

    /**
     * @param string $channel
     *
     * @return \Amp\Promise
     *
     * @throws \Error
     */
    private function unlisten(string $channel): Promise {
        \assert(isset($this->listeners[$channel]), "Not listening on that channel");

        $emitter = $this->listeners[$channel];
        unset($this->listeners[$channel]);

        if (empty($this->listeners) && $this->deferred === null) {
            Loop::disable($this->poll);
        }

        $promise = new Coroutine($this->send([$this->handle, "unlistenAsync"], $channel));
        $promise->onResolve([$emitter, "complete"]);
        return $promise;
    }
}
