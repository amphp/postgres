<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ CallableMaker, Coroutine, Deferred, function pipe };
use Interop\Async\{ Awaitable, Loop };
use pq;

class PqExecutor implements Executor {
    use CallableMaker;
    
    /** @var \pq\Connection PostgreSQL connection object. */
    private $handle;

    /** @var \Amp\Deferred|null */
    private $delayed;
    
    /** @var \Amp\Deferred */
    private $busy;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var callable */
    private $send;
    
    /** @var callable */
    private $fetch;
    
    /** @var callable */
    private $release;
    
    /**
     * Connection constructor.
     *
     * @param \pq\Connection $handle
     */
    public function __construct(pq\Connection $handle) {
        $this->handle = $handle;

        $deferred = &$this->delayed;
        
        $this->poll = Loop::onReadable($this->handle->socket, static function ($watcher) use (&$deferred, $handle) {
            if ($handle->poll() === pq\Connection::POLLING_FAILED) {
                $deferred->fail(new FailureException($handle->errorMessage));
                return;
            }

            if (!$handle->busy) {
                $deferred->resolve($handle->getResult());
                return;
            }

            // Reading not done, listen again.
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
            } catch (pg\Exception $exception) {
                throw new FailureException($this->handle->errorMessage, 0, $exception);
            }
    
            $this->delayed = new Deferred;
    
            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }
    
            try {
                $result = yield $this->delayed->getAwaitable();
            } finally {
                $this->delayed = null;
                Loop::disable($this->poll);
                Loop::disable($this->await);
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
            $this->delayed = new Deferred;
    
            Loop::enable($this->poll);
    
            try {
                $result = yield $this->delayed->getAwaitable();
            } finally {
                $this->delayed = null;
                Loop::disable($this->poll);
            }
        }
        
        switch ($result->status) {
            case pq\Result::TUPLES_OK: // No more rows in result set.
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
}
