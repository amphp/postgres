<?php

namespace Amp\Postgres;

interface Quoter
{
    /**
     * Quotes (escapes) the given string for use as a string literal or identifier in a query. This method wraps the
     * string in single quotes, so additional quotes should not be added in the query.
     *
     * @param string $data Unquoted data.
     *
     * @return string Quoted string wrapped in single quotes.
     *
     * @throws \Error If the connection to the database has been closed.
     */
    public function quoteString(string $data): string;

    /**
     * Quotes (escapes) the given string for use as a name or identifier in a query.
     *
     * @param string $name Unquoted identifier.
     *
     * @return string Quoted identifier.
     *
     * @throws \Error If the connection to the database has been closed.
     */
    public function quoteName(string $name): string;
}
