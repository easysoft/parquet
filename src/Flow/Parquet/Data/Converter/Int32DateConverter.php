<?php

declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class Int32DateConverter implements Converter
{
    /**
     * @param mixed $data
     */
    public function fromParquetType($data) : \DateTimeImmutable
    {
        return $this->numberOfDaysToDateTime($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT32 && (($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) === LogicalType::DATE) {
            return true;
        }

        return false;
    }

    /**
     * @param mixed $data
     */
    public function toParquetType($data) : int
    {
        return $this->dateTimeToNumberOfDays($data);
    }

    /**
     * @param \DateTime|\DateTimeImmutable $date
     */
    private function dateTimeToNumberOfDays($date) : int
    {
        $epoch = new \DateTimeImmutable('1970-01-01 00:00:00 UTC');
        $interval = $epoch->diff($date->setTime(0, 0, 0, 0));

        return (int) $interval->format('%a');
    }

    private function numberOfDaysToDateTime(int $data) : \DateTimeImmutable
    {
        return (new \DateTimeImmutable('1970-01-01 00:00:00 UTC'))->add(new \DateInterval('P' . $data . 'D'));
    }
}
