<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ CallableMaker, Coroutine, Deferred, function pipe };
use Interop\Async\Promise;

abstract class AbstractConnection implements Connection {
    use CallableMaker;
    
    /** @var \Amp\Postgres\Executor */
    private $executor;
    
    /** @var \Amp\Deferred|null Used to only allow one transaction at a time. */
    private $busy;
    
    /** @var callable */
    private $release;
    
    /**
     * @param string $connectionString
     * @param int $timeout Timeout until the connection attempt fails.
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\Connection>
     */
    abstract public static function connect(string $connectionString, int $timeout = null): Promise;
    
    /**
     * @param $executor;
     */
    public function __construct(Executor $executor) {
        $this->executor = $executor;
        $this->release = $this->callableFromInstanceMethod("release");
    }

    /**
     * @param callable $method Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Generator
     *
     * @throws \Amp\Postgres\FailureException
     */
    private function send(callable $method, ...$args): \Generator {
        while ($this->busy !== null) {
            yield $this->busy->promise();
        }
        
        return $method(...$args);
    }
    
    /**
     * Releases the transaction lock.
     */
    private function release() {
        $busy = $this->busy;
        $this->busy = null;
        $busy->resolve();
    }
    
    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return new Coroutine($this->send([$this->executor, "query"], $sql));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Promise {
        return new Coroutine($this->send([$this->executor, "execute"], $sql, ...$params));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return new Coroutine($this->send([$this->executor, "prepare"], $sql));
    }
    
    
    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return new Coroutine($this->send([$this->executor, "notify"], $channel, $payload));
    }
    
    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise {
        return new Coroutine($this->send([$this->executor, "listen"], $channel));
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        switch ($isolation) {
            case Transaction::UNCOMMITTED:
                $promise = $this->query("BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
                break;
        
            case Transaction::COMMITTED:
                $promise = $this->query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                break;
        
            case Transaction::REPEATABLE:
                $promise = $this->query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                break;
        
            case Transaction::SERIALIZABLE:
                $promise = $this->query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                break;
        
            default:
                throw new \Error("Invalid transaction type");
        }
    
        return pipe($promise, function (CommandResult $result) use ($isolation) {
            $this->busy = new Deferred;
            $transaction = new Transaction($this->executor, $isolation);
            $transaction->onComplete($this->release);
            return $transaction;
        });
    }
}
