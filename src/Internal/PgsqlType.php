<?php

namespace Amp\Postgres\Internal;

/** @internal */
final class PgsqlType
{
    private static ?self $default = null;

    public function __construct(
        public readonly string $type,
        public readonly string $delimiter,
        public readonly int $element,
    ) {
    }

    public static function getDefaultType(): self
    {
        return self::$default ??= new self('S', ',', 0);
    }
}
