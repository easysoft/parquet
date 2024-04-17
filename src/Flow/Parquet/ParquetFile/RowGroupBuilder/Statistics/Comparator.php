<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\Statistics;

use Flow\Parquet\Data\Converter\TimeConverter;

final class Comparator
{
    /**
     * @param mixed $value
     * @param mixed $nextValue
     */
    public function isGreaterThan($value, $nextValue) : bool
    {
        if ($value === null) {
            return false;
        }

        if ($nextValue === null) {
            return true;
        }

        if (\gettype($value) !== \gettype($nextValue)) {
            throw new \RuntimeException(\sprintf('Cannot compare %s with %s', \gettype($value), \gettype($nextValue)));
        }

        if ($value instanceof \DateInterval) {
            $value = (new TimeConverter())->toParquetType($value);
            $nextValue = (new TimeConverter())->toParquetType($nextValue);
        }

        return $value > $nextValue;
    }

    /**
     * @param mixed $value
     * @param mixed $nextValue
     */
    public function isLessThan($value, $nextValue) : bool
    {
        if ($value === null) {
            return false;
        }

        if ($nextValue === null) {
            return true;
        }

        if (\gettype($value) !== \gettype($nextValue)) {
            throw new \RuntimeException(\sprintf('Cannot compare %s with %s', \gettype($value), \gettype($nextValue)));
        }

        if ($value instanceof \DateInterval) {
            $value = (new TimeConverter())->toParquetType($value);
            $nextValue = (new TimeConverter())->toParquetType($nextValue);
        }

        return $value < $nextValue;
    }
}
