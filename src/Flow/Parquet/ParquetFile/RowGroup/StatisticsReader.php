<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ParquetFile\Data\PlainValueUnpacker;
use Flow\Parquet\ParquetFile\Schema\{ColumnPrimitiveType, FlatColumn};
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsReader
{
    /**
     * @readonly
     * @var \Flow\Parquet\ParquetFile\Statistics
     */
    private $statistics;
    public function __construct(Statistics $statistics)
    {
        $this->statistics = $statistics;
    }

    public function distinctCount() : ?int
    {
        return $this->statistics->distinctCount;
    }

    /**
     * @return mixed
     */
    public function max(FlatColumn $column)
    {
        if ($this->statistics->max === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->max, 'UTF-8')) {
            return $this->statistics->max;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->max))))->unpack($column, 1)[0];
    }

    /**
     * @return mixed
     */
    public function maxValue(FlatColumn $column)
    {
        if ($this->statistics->maxValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->maxValue, 'UTF-8')) {
            return $this->statistics->maxValue;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->maxValue))))->unpack($column, 1)[0];
    }

    /**
     * @return mixed
     */
    public function min(FlatColumn $column)
    {
        if ($this->statistics->min === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->min, 'UTF-8')) {
            return $this->statistics->min;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->min))))->unpack($column, 1)[0];
    }

    /**
     * @return mixed
     */
    public function minValue(FlatColumn $column)
    {
        if ($this->statistics->minValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->minValue, 'UTF-8')) {
            return $this->statistics->minValue;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->minValue))))->unpack($column, 1)[0];
    }

    public function nullCount() : ?int
    {
        return $this->statistics->nullCount;
    }
}
