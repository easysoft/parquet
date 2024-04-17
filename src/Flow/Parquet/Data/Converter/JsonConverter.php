<?php

declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType};

final class JsonConverter implements Converter
{
    /**
     * @param mixed $data
     */
    public function fromParquetType($data) : string
    {
        if (!\is_string($data)) {
            throw new RuntimeException('Json must be read as a string from Parquet file');
        }

        return $data;
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ((($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) === LogicalType::JSON) {
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
            throw new RuntimeException('Json must be written as a string from Parquet file');
        }

        return $data;
    }
}
