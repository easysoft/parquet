<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use function Flow\Parquet\array_flatten;
use Flow\Parquet\Data\ObjectToString;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Statistics\Comparator;

final class DataPageV2Statistics
{
    /**
     * @var \Flow\Parquet\ParquetFile\RowGroupBuilder\Statistics\Comparator
     */
    private $comparator;

    /**
     * @var mixed
     */
    private $max;

    /**
     * @var mixed
     */
    private $min;

    /**
     * @var int
     */
    private $nullCount;

    /**
     * @var mixed[]
     */
    private $values = [];

    /**
     * @var int
     */
    private $valuesCount;

    public function __construct()
    {
        $this->nullCount = 0;
        $this->valuesCount = 0;
        $this->min = null;
        $this->max = null;
        $this->comparator = new Comparator();
    }

    /**
     * @param string|int|float|mixed[]|bool|object|null $value
     */
    public function add($value) : void
    {
        if (\is_array($value)) {
            $value = array_flatten($value);
        }

        if (\is_array($value)) {
            $this->valuesCount += \count($value);
        } else {
            $this->valuesCount++;
        }

        if ($value === null) {
            $this->nullCount++;

            return;
        }

        if (\is_array($value)) {
            foreach ($value as $val) {

                if ($this->comparator->isLessThan($val, $this->min)) {
                    $this->min = $val;
                }

                if ($this->comparator->isGreaterThan($val, $this->max)) {
                    $this->max = $val;
                }

                $this->values[] = \is_object($val) ? ObjectToString::toString($val) : $val;
            }
        } else {
            if ($this->comparator->isLessThan($value, $this->min)) {
                $this->min = $value;
            }

            if ($this->comparator->isGreaterThan($value, $this->max)) {
                $this->max = $value;
            }

            $this->values[] = \is_object($value) ? ObjectToString::toString($value) : $value;
        }
    }

    public function distinctCount() : int
    {
        if ([] === $this->values) {
            return 0;
        }

        return \count(\array_unique($this->values));
    }

    /**
     * @return mixed
     */
    public function max()
    {
        return $this->max;
    }

    /**
     * @return mixed
     */
    public function min()
    {
        return $this->min;
    }

    public function nullCount() : int
    {
        return $this->nullCount;
    }

    public function values() : array
    {
        return $this->values;
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
