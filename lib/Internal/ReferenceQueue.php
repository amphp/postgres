<?php

namespace Amp\Postgres\Internal;

use Amp\Loop;

class ReferenceQueue {
    /** @var callable[] */
    private $onDestruct = [];

    /** @var int */
    private $refCount = 1;

    public function onDestruct(callable $onDestruct) {
        if (!$this->refCount) {
            $onDestruct();
            return;
        }

        $this->onDestruct[] = $onDestruct;
    }

    public function reference() {
        ++$this->refCount;
    }

    public function unreference() {
        if (!$this->refCount) {
            return;
        }

        if (--$this->refCount) {
            return;
        }

        foreach ($this->onDestruct as $callback) {
            try {
                $callback();
            } catch (\Throwable $exception) {
                Loop::defer(function () use ($exception) {
                    throw $exception; // Rethrow to event loop error handler.
                });
            }
        }
        $this->onDestruct = null;
    }

    public function isReferenced(): bool {
        return $this->refCount;
    }
}
