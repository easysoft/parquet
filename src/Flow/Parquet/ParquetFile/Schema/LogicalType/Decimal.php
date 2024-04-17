<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

final class Decimal
{
    /**
     * @readonly
     * @var int
     */
    private $scale;
    /**
     * @readonly
     * @var int
     */
    private $precision;
    public function __construct(int $scale, int $precision)
    {
        $this->scale = $scale;
        $this->precision = $precision;
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DecimalType $thrift) : self
    {
        return new self(
            $thrift->scale,
            $thrift->precision
        );
    }

    public function precision() : int
    {
        return $this->precision;
    }

    public function scale() : int
    {
        return $this->scale;
    }
}
