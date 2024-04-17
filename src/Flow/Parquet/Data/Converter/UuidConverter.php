<?php

declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType};

final class UuidConverter implements Converter
{
    /**
     * @param mixed $data
     */
    public function fromParquetType($data) : string
    {
        if (!\is_string($data)) {
            throw new RuntimeException('UUID must be read as a string from Parquet file');
        }

        return $data;
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ((($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) === LogicalType::UUID) {
            return true;
        }

        return false;
    }

    /**
     * @param mixed $data
     */
    public function toParquetType($data) : string
    {
        if (!\is_string($data)) {
            throw new RuntimeException('UUID must be written as a string from Parquet file');
        }

        return $data;
    }
}
