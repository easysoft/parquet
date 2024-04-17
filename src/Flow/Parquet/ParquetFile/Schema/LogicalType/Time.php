<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\Thrift\TimeType;

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 */
final class Time
{
    /**
     * @readonly
     * @var bool
     */
    private $isAdjustedToUTC;
    /**
     * @readonly
     * @var bool
     */
    private $millis;
    /**
     * @readonly
     * @var bool
     */
    private $micros;
    /**
     * @readonly
     * @var bool
     */
    private $nanos;
    public function __construct(bool $isAdjustedToUTC, bool $millis, bool $micros, bool $nanos)
    {
        $this->isAdjustedToUTC = $isAdjustedToUTC;
        $this->millis = $millis;
        $this->micros = $micros;
        $this->nanos = $nanos;
    }

    public static function fromThrift(TimeType $timestamp) : self
    {
        return new self(
            $timestamp->isAdjustedToUTC,
            $timestamp->unit->MILLIS !== null,
            $timestamp->unit->MICROS !== null,
            $timestamp->unit->NANOS !== null
        );
    }

    public function isAdjustedToUTC() : bool
    {
        return $this->isAdjustedToUTC;
    }

    public function micros() : bool
    {
        return $this->micros;
    }

    public function millis() : bool
    {
        return $this->millis;
    }

    public function nanos() : bool
    {
        return $this->nanos;
    }
}
