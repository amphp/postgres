<?php

namespace Amp\Postgres;

final class PooledListener implements Listener, \IteratorAggregate
{
    private Listener $listener;

    /** @var callable|null */
    private $release;

    public function __construct(Listener $listener, callable $release)
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
        if ($this->listener->isListening()) {
            $this->unlisten(); // Invokes $this->release callback.
        }
    }

    public function continue(): ?Notification
    {
        return $this->listener->continue();
    }

    public function dispose(): void
    {
        $this->listener->dispose();
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
            throw new \Error("Already unlistened on this channel");
        }

        $release = $this->release;
        $this->release = null;

        try {
            $this->listener->unlisten();
        } finally {
            $release();
        }
    }
}
