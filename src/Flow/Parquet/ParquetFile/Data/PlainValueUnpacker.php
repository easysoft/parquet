<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class PlainValueUnpacker
{
    /**
     * @readonly
     * @var \Flow\Parquet\BinaryReader
     */
    private $reader;
    public function __construct(BinaryReader $reader)
    {
        $this->reader = $reader;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     *
     * @return array<mixed>
     */
    public function unpack(FlatColumn $column, int $total) : array
    {
        switch ($column->type()) {
            case PhysicalType::INT32:
                return $this->reader->readInts32($total);
            case PhysicalType::INT64:
                return $this->reader->readInts64($total);
            case PhysicalType::INT96:
                return $this->reader->readInts96($total);
            case PhysicalType::FLOAT:
                return $this->reader->readFloats($total);
            case PhysicalType::DOUBLE:
                return $this->reader->readDoubles($total);
            case PhysicalType::BYTE_ARRAY:
                switch (($nullsafeVariable1 = $column->logicalType()) ? $nullsafeVariable1->name() : null) {
                    case LogicalType::STRING:
                    case LogicalType::JSON:
                    case LogicalType::UUID:
                        return $this->reader->readStrings($total);
                    default:
                        return $this->reader->readByteArrays($total);
                }
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch (($nullsafeVariable2 = $column->logicalType()) ? $nullsafeVariable2->name() : null) {
                    case LogicalType::DECIMAL:
                        return $this->reader->readDecimals($total, $column->typeLength(), ($nullsafeVariable3 = ($nullsafeVariable4 = $column->logicalType()) ? $nullsafeVariable4->decimalData() : null) ? $nullsafeVariable3->precision() : null, ($nullsafeVariable5 = ($nullsafeVariable6 = $column->logicalType()) ? $nullsafeVariable6->decimalData() : null) ? $nullsafeVariable5->scale() : null);
                    default:
                        throw new RuntimeException('Unsupported logical type ' . ((($nullsafeVariable7 = $column->logicalType()) ? $nullsafeVariable7->name() : null) ?: 'null') . ' for FIXED_LEN_BYTE_ARRAY');
                }
            case PhysicalType::BOOLEAN:
                return $this->reader->readBooleans($total);
        }
    }
}
