<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

final class Statistics
{
    /**
     * @readonly
     * @var string|null
     */
    public $max;
    /**
     * @readonly
     * @var string|null
     */
    public $min;
    /**
     * @readonly
     * @var int|null
     */
    public $nullCount;
    /**
     * @readonly
     * @var int|null
     */
    public $distinctCount;
    /**
     * @readonly
     * @var string|null
     */
    public $maxValue;
    /**
     * @readonly
     * @var string|null
     */
    public $minValue;
    public function __construct(?string $max, ?string $min, ?int $nullCount, ?int $distinctCount, ?string $maxValue, ?string $minValue)
    {
        $this->max = $max;
        $this->min = $min;
        $this->nullCount = $nullCount;
        $this->distinctCount = $distinctCount;
        $this->maxValue = $maxValue;
        $this->minValue = $minValue;
    }
    public static function fromThrift(\Flow\Parquet\Thrift\Statistics $thrift) : self
    {
        return new self($thrift->max, $thrift->min, $thrift->null_count, $thrift->distinct_count, $thrift->max_value, $thrift->min_value);
    }

    public function toThrift() : \Flow\Parquet\Thrift\Statistics
    {
        return new \Flow\Parquet\Thrift\Statistics([
            'max' => $this->max,
            'min' => $this->min,
            'null_count' => $this->nullCount,
            'distinct_count' => $this->distinctCount,
            'max_value' => $this->maxValue,
            'min_value' => $this->minValue,
        ]);
    }
}
