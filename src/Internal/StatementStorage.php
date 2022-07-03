<?php

namespace Amp\Postgres\Internal;

use Amp\Future;

/**
 * @template T
 *
 * @internal
 */
final class StatementStorage
{
    public int $refCount = 1;

    /**
     * @param string $sql
     * @param Future<T>|Future<void> $future
     */
    public function __construct(
        public readonly string $sql,
        public Future $future,
    ) {
    }
}
