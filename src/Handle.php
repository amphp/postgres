<?php

namespace Amp\Postgres;

interface Handle extends Receiver, Quoter
{
    const STATEMENT_NAME_PREFIX = "amp_";
}
