<?php

namespace Amp\Postgres;

/**
 * Class used to wrap binary BYTEA fields.
 *
 * Strings wrapped in this class will be automatically escaped according to the current postgres settings.
 */
final class ByteA
{
    /**
     * Input data.
     */
    private string $string;
    /**
     * Wrap byteA field.
     *
     * @param string $string
     */
    public function __construct(string $string)
    {
        $this->string = $string;
    }

    /**
     * Get string data.
     *
     * @return string
     */
    public function getString(): string
    {
        return $this->string;
    }
}
