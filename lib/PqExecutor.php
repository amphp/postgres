<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ CallableMaker, Coroutine, Deferred, Postponed, function pipe };
use Interop\Async\{ Awaitable, Loop };
use pq;

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
    
    /** @var \Amp\Postponed[] */
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
                return;
            }

            if (!$handle->busy) {
                if (empty($listeners)) {
                    Loop::disable($watcher);
                }
                $deferred->resolve($handle->getResult());
            }
        });

        $this->await = Loop::onWritable($this->handle->socket, static function ($watcher) use (&$deferred, $handle) {
            if (!$handle->flush()) {
                return; // Not finished sending data, listen again.
            }
            
            Loop::disable($watcher);
        });
        
        Loop::disable($this->poll);
        Loop::disable($this->await);

        $this->send = $this->callableFromInstanceMethod("send");
        $this->fetch = $this->callableFromInstanceMethod("fetch");
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
        while ($this->busy !== null) {
            yield $this->busy->getAwaitable();
        }
        
        $this->busy = new Deferred;

        try {
            try {
                $handle = $method(...$args);
            } catch (pq\Exception $exception) {
                throw new FailureException($this->handle->errorMessage, 0, $exception);
            }
    
            $this->deferred = new Deferred;
    
            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }
    
            try {
                $result = yield $this->deferred->getAwaitable();
            } finally {
                $this->deferred = null;
            }
    
            if ($handle instanceof pq\Statement) {
                return new PqStatement($handle, $this->send);
            }
            
            if (!$result instanceof pq\Result) {
                throw new FailureException("Unknown query result");
            }
        } finally {
            $this->release();
        }
    
        switch ($result->status) {
            case pq\Result::EMPTY_QUERY:
                throw new QueryError("Empty query string");
        
            case pq\Result::COMMAND_OK:
                return new PqCommandResult($result);
        
            case pq\Result::TUPLES_OK:
                return new PqBufferedResult($result);
        
            CASE pq\Result::SINGLE_TUPLE:
                $result = new PqUnbufferedResult($this->fetch, $result);
                $result->onComplete($this->release);
                $this->busy = new Deferred;
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
                $result = yield $this->deferred->getAwaitable();
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
        $busy = $this->busy;
        $this->busy = null;
        $busy->resolve();
    }
    
    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Awaitable {
        return new Coroutine($this->send([$this->handle, "execAsync"], $sql));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Awaitable {
        return new Coroutine($this->send([$this->handle, "execParamsAsync"], $sql, $params));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Awaitable {
        return new Coroutine($this->send([$this->handle, "prepareAsync"], $sql, $sql));
    }
    
    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Awaitable {
        return new Coroutine($this->send([$this->handle, "notifyAsync"], $channel, $payload));
    }
    
    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Awaitable {
        $postponed = new Postponed;
        $awaitable = new Coroutine($this->send(
            [$this->handle, "listenAsync"],
            $channel,
            static function (string $channel, string $message, int $pid) use ($postponed) {
                $notification = new Notification;
                $notification->channel = $channel;
                $notification->pid = $pid;
                $notification->payload = $message;
                $postponed->emit($notification);
            }));
        
        return pipe($awaitable, function () use ($postponed, $channel) {
            $this->listeners[$channel] = $postponed;
            Loop::enable($this->poll);
            return new Listener($postponed->getObservable(), $channel, $this->unlisten);
        });
    }
    
    /**
     * @param string $channel
     *
     * @return \Interop\Async\Awaitable
     *
     * @throws \Error
     */
    private function unlisten(string $channel): Awaitable {
        if (!isset($this->listeners[$channel])) {
            throw new \Error("Not listening on that channel");
        }
        
        $postponed = $this->listeners[$channel];
        unset($this->listeners[$channel]);
    
        if (empty($this->listeners) && $this->deferred === null) {
            Loop::disable($this->poll);
        }
    
        $awaitable = new Coroutine($this->send([$this->handle, "unlistenAsync"], $channel));
        $awaitable->when(function () use ($postponed) {
            $postponed->resolve();
        });
        return $awaitable;
    }
}
