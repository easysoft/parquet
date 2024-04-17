<?php

declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class Int32DateTimeConverter implements Converter
{
    /**
     * @param mixed $data
     */
    public function fromParquetType($data) : \DateTimeImmutable
    {
        return $this->millisecondsToDateTimeImmutable($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT32 && (($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) === LogicalType::TIMESTAMP) {
            return true;
        }

        return false;
    }

    /**
     * @param mixed $data
     */
    public function toParquetType($data) : int
    {
        return $this->dateTimeToMicroseconds($data);
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     */
    private function dateTimeToMicroseconds(\DateTimeInterface $dateTime) : int
    {
        $microseconds = \number_format((((int) $dateTime->format('u')) / 1000), 0, '', '') . '000';

        return (int) \bcadd(\bcmul($dateTime->format('U'), '1000000'), $microseconds);
    }

    private function millisecondsToDateTimeImmutable(int $microseconds) : \DateTimeImmutable
    {
        $seconds = ($microseconds / 1000000);
        $fraction = \str_pad((string) ($microseconds % 1000000), 6, '0', STR_PAD_LEFT);

        $dateTime = \DateTimeImmutable::createFromFormat('U.u', \sprintf('%d.%s', $seconds, $fraction));

        if ($dateTime === false) {
            throw new RuntimeException('Failed to convert INT32 to DateTime, given microseconds: ' . \json_encode(['microseconds' => $microseconds, 'fraction' => $fraction], 0));
        }

        return $dateTime;
    }
}
