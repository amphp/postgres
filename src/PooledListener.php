<?php

namespace Amp\Postgres;

use Revolt\EventLoop;

final class PooledListener implements Listener, \IteratorAggregate
{
    private readonly Listener $listener;

    /** @var \Closure|null */
    private ?\Closure $release;

    public function __construct(Listener $listener, \Closure $release)
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
