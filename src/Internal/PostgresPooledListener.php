<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresListener;
use Revolt\EventLoop;

/** @internal  */
final class PostgresPooledListener implements PostgresListener, \IteratorAggregate
{
    private readonly PostgresListener $listener;

    private ?\Closure $release;

    /**
     * @param \Closure():void $release
     */
    public function __construct(PostgresListener $listener, \Closure $release)
    {
        $this->listener = $listener;
        $this->release = $release;

        if (!$this->listener->isListening()) {
            ($this->release)();
            $this->release = null;
        }
    }

    public function __destruct()
    {
        if ($this->listener->isListening() && $this->release) {
            $listener = $this->listener;
            $release = $this->release;
            EventLoop::queue(static function () use ($listener, $release): void {
                try {
                    $listener->unlisten();
                } finally {
                    EventLoop::queue($release);
                }
            });
            $this->release = null;
        }
    }

    public function getIterator(): \Traversable
    {
        // Using a Generator to keep a reference to $this.
        yield from $this->listener;
    }

    public function getChannel(): string
    {
        return $this->listener->getChannel();
    }

    public function isListening(): bool
    {
        return $this->listener->isListening();
    }

    public function unlisten(): void
    {
        if (!$this->release) {
            return;
        }

        $release = $this->release;
        $this->release = null;

        try {
            $this->listener->unlisten();
        } finally {
            EventLoop::queue($release);
        }
    }
}
