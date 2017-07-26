<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Emitter;
use Amp\Loop;
use Amp\Promise;
use pq;
use function Amp\call;
use function Amp\coroutine;

class PqExecutor implements Executor {
    use CallableMaker;

    /** @var \pq\Connection PostgreSQL connection object. */
    private $handle;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var \Amp\Deferred */
    private $busy;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var \Amp\Emitter[] */
    private $listeners;

    /** @var callable */
    private $send;

    /** @var callable */
    private $fetch;

    /** @var callable */
    private $unlisten;

    /** @var callable */
    private $release;

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

            if (!$handle->busy && empty($listeners)) {
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

        $this->send = $this->callableFromInstanceMethod("send");
        $this->fetch = coroutine($this->callableFromInstanceMethod("fetch"));
        $this->unlisten = $this->callableFromInstanceMethod("unlisten");
        $this->release = $this->callableFromInstanceMethod("release");
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
        if ($this->busy) {
            throw new PendingOperationError;
        }

        $this->busy = true;

        try {
            $handle = $method(...$args);

            $this->deferred = new Deferred;

            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }

            try {
                $result = yield $this->deferred->promise();
            } finally {
                $this->deferred = null;
            }

            if ($handle instanceof pq\Statement) {
                return new PqStatement($handle, $this->send);
            }

            if (!$result instanceof pq\Result) {
                throw new FailureException("Unknown query result");
            }
        } catch (pq\Exception $exception) {
            throw new FailureException($this->handle->errorMessage, 0, $exception);
        } finally {
            $this->busy = false;
        }

        switch ($result->status) {
            case pq\Result::EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case pq\Result::COMMAND_OK:
                return new PqCommandResult($result);

            case pq\Result::TUPLES_OK:
                return new PqBufferedResult($result);

            case pq\Result::SINGLE_TUPLE:
                $this->busy = true;
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
        $this->busy = false;
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
        return new Coroutine($this->send([$this->handle, "prepareAsync"], "amphp" .sha1($sql), $sql));
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
                    });
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
        if (!isset($this->listeners[$channel])) {
            throw new \Error("Not listening on that channel");
        }

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
